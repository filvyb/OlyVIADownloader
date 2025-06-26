import faststreams/inputs
import asyncdispatch
import strutils, options
import os

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums, context]
import DotNimRemoting/msnrbf/records/[methodinv, member, arrays]

proc getFileList*(client: NrtpTcpClient, serverURL: string, databaseGUID: string, 
                  entryGUID: string = ""): Future[tuple[status: int32, fileNames: seq[string]]] {.async.} =
  ## Gets the list of files from the remote file server
  ## Parameters:
  ##   serverURL - The server URL/identifier
  ##   databaseGUID - The database GUID
  ##   entryGUID - Optional entry GUID (can be empty string for null)
  ## Returns a tuple with:
  ##   status - Status code (1 for success based on the packet)
  ##   fileNames - Array of file names
  
  # Set the path to the RemoteFileServer object
  client.setPath("RemotingWrapper.RemoteFileServer")

  # Create serialization context
  let ctx = newSerializationContext()
  
  # Create method call with ArgsInline flag
  var flags: MessageFlags = {MessageFlag.ArgsInline, MessageFlag.NoContext}
  
  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"
  
  var call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("GetFileList"),
    typeName: newStringValueWithCode(fullTypeName)
  )
  
  var args: seq[ValueWithCode] = @[]
  args.add(toValueWithCode(stringValue(serverURL)))      # strServerURL
  args.add(toValueWithCode(stringValue(databaseGUID)))   # strDatabaseGUID
  if entryGUID == "":
    args.add(ValueWithCode(primitiveType: ptNull, value: PrimitiveValue(kind: ptNull)))
  else:
    args.add(toValueWithCode(stringValue(entryGUID)))
  
  call.args = args
  
  # Create the message
  let msg = newRemotingMessage(ctx, methodCall = some(call))
  
  # Serialize and send
  let requestData = serializeRemotingMessage(msg, ctx)
  
  echo "Getting file list from server..."
  echo "  Server URL: ", serverURL
  echo "  Database GUID: ", databaseGUID
  if entryGUID != "":
    echo "  Entry GUID: ", entryGUID
  
  let responseData = await client.invoke("GetFileList", fullTypeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Extract the return status
    var status: int32 = -1
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      status = ret.returnValue.value.int32Val
      if status != 1:
        echo "GetFileList failed with status code: ", status
        return (status, @[])
    elif MessageFlag.ReturnValueInArray in ret.messageEnum:
      # Return value is in the array
      if responseMsg.methodCallArray.len > 0:
        let firstItem = responseMsg.methodCallArray[0]
        if firstItem.kind == rvPrimitive and firstItem.primitiveVal.kind == ptInt32:
          status = firstItem.primitiveVal.int32Val
    
    # 1. BinaryMethodReturn with status code (1 = success)
    # 2. ArraySingleObject with 3 items:
    #    - Items 0-1: null values (for the first two input parameters)
    #    - Item 2: MemberReference to the file names array
    # 3. ArraySingleString containing the actual file names
    
    # Find the file names array in the referenced records
    var fileNames: seq[string] = @[]
    
    # The methodCallArray should contain the reference to the string array
    if responseMsg.methodCallArray.len >= 3:
      # The reference to the string array should be the last item (index 2)
      let refItem = responseMsg.methodCallArray[2]
      if refItem.kind == rvReference:
        let refId = refItem.idRef
        
        # Find the referenced array in the records
        for record in responseMsg.referencedRecords:
          if record.kind == rvArray:
            let arrayRecord = record.arrayVal.record
            if arrayRecord.kind == rtArraySingleString:
              let arrayInfo = arrayRecord.arraySingleString.arrayInfo
              if arrayInfo.objectId == refId:
                for elem in record.arrayVal.elements:
                  if elem.kind == rvString:
                    fileNames.add(elem.stringVal.value)
                
                echo "Successfully retrieved ", fileNames.len, " files"
                return (status, fileNames)
    
    # If we didn't find the array through references, check referenced records directly
    for record in responseMsg.referencedRecords:
      if record.kind == rvArray:
        let arrayRecord = record.arrayVal.record
        if arrayRecord.kind == rtArraySingleString:
          for elem in record.arrayVal.elements:
            if elem.kind == rvString:
              fileNames.add(elem.stringVal.value)
          
          echo "Successfully retrieved ", fileNames.len, " files"
          return (status, fileNames)
    
    echo "Warning: Could not find file names in response, but status was success"
    return (status, @[])
  
  raise newException(IOError, "Failed to get file list")

proc isImageValid*(client: NrtpTcpClient, imageId: string, databaseGuid: string): Future[bool] {.async.} =
  ## Checks if an image is valid in the image server
  ## Parameters:
  ##   imageId - The image ID (GUID format)
  ##   databaseGuid - The database GUID
  ## Returns an Int32 status (1 for valid, 0 for invalid)
  
  # Set the path to the image server object
  client.setPath("DotNetServer.WebImageAccess")

  let typeName = "RemoteImageAccess.IImage, RemoteImageAccess"
  
  # Create arguments: imageId and databaseGuid
  let args = @[
    stringValue(imageId),      # strImageID parameter
    stringValue(databaseGuid)  # strDatabaseGUID parameter
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "IsValid",
    typeName = typeName,
    args = args
  )
  
  echo "Checking if image is valid - ID: ", imageId
  let responseData = await client.invoke("IsValid", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let isValid = ret.returnValue.value.int32Val
      if isValid == 1:
        echo "Image is valid"
        return true
      else:
        echo "Image is not valid"
        return false
  
  echo "Failed to check image validity, unexpected response"
  raise newException(IOError, "Failed to check image validity")

proc openFile*(client: NrtpTcpClient, serverURL: string, databaseGUID: string, 
               fileName: string, accessMode: uint32 = 0x80000000'u32): Future[int32] {.async.} =
  ## Opens a file on the remote file server
  ## Parameters:
  ##   serverURL - The server URL/identifier (GUID format)
  ##   databaseGUID - The database GUID
  ##   fileName - The name of the file to open
  ##   accessMode - The access mode (default: 0x80000000)
  ## Returns the status code (0 for success)
  
  client.setPath("RemotingWrapper.RemoteFileServer")

  let ctx = newSerializationContext()
  
  var flags: MessageFlags = {MessageFlag.ArgsInline, MessageFlag.NoContext}
  
  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"
  
  var call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("Open"),
    typeName: newStringValueWithCode(fullTypeName)
  )
  
  var args: seq[ValueWithCode] = @[]
  args.add(toValueWithCode(stringValue(serverURL)))      # strServerURL
  args.add(toValueWithCode(stringValue(databaseGUID)))   # strDatabaseGUID
  args.add(toValueWithCode(stringValue(fileName)))       # strFileName
  args.add(toValueWithCode(uint32Value(accessMode)))     # nAccessMode
  
  call.args = args
  
  # Create the message
  let msg = newRemotingMessage(ctx, methodCall = some(call))
  
  # Serialize and send
  let requestData = serializeRemotingMessage(msg, ctx)
  
  echo "Opening file on server..."
  echo "  Server URL: ", serverURL
  echo "  Database GUID: ", databaseGUID
  echo "  File Name: ", fileName
  echo "  Access Mode: 0x", accessMode.toHex()
  
  let responseData = await client.invoke("Open", fullTypeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Extract the return status
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let status = ret.returnValue.value.int32Val
      if status == 0:
        echo "File opened successfully"
      else:
        echo "Failed to open file with status code: ", status
      return status
  
  raise newException(IOError, "Failed to open file")

proc getFileSize*(client: NrtpTcpClient, serverGUID: string, entryGUID: string, 
                  fileName: string): Future[tuple[status: int32, fileSize: int64]] {.async.} =
  ## Gets the size of a file from the remote file server
  ## Parameters:
  ##   serverGUID - The server GUID
  ##   entryGUID - The entry GUID
  ##   fileName - The name of the file
  ## Returns a tuple with:
  ##   status - Status code (0 for success)
  ##   fileSize - Size of the file in bytes
  
  # Set the path to the RemoteFileServer object
  client.setPath("RemotingWrapper.RemoteFileServer")

  # Create method call with the required parameters
  let typeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper"
  
  # Create arguments: serverGUID, entryGUID, fileName, and placeholder for ref parameter
  let args = @[
    stringValue(serverGUID),    # strServer parameter
    stringValue(entryGUID),     # strEntryGUID parameter
    stringValue(fileName),      # strFileName parameter
    int64Value(0)              # Placeholder for ref strFileSize parameter
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "GetFileSize",
    typeName = typeName,
    args = args
  )
  
  echo "Getting file size for: ", fileName
  echo "  Server GUID: ", serverGUID
  echo "  Entry GUID: ", entryGUID
  
  let responseData = await client.invoke("GetFileSize", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    var status: int32 = -1
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      status = ret.returnValue.value.int32Val
      if status != 0:
        echo "GetFileSize failed with status code: ", status
        return (status, 0'i64)
    
    # Check if we have arguments in the response
    if MessageFlag.ArgsInline in ret.messageEnum and ret.args.len >= 4:
      # The fourth parameter is the output file size
      let fileSize = ret.args[3].value.int64Val
      
      echo "File size: ", fileSize, " bytes (", formatFloat(fileSize.float / 1024.0 / 1024.0, ffDecimal, 2), " MB)"
      return (status, fileSize)
    
    raise newException(IOError, "Invalid response format: not enough arguments returned")
  
  raise newException(IOError, "Failed to get file size")

proc closeFile*(client: NrtpTcpClient, serverGUID: string, databaseGUID: string, 
                fileName: string): Future[int32] {.async.} =
  ## Closes a file on the remote file server
  ## Parameters:
  ##   serverGUID - The server GUID
  ##   databaseGUID - The database GUID
  ##   fileName - The name of the file to close
  ## Returns the status code (0 for success)
  
  # Set the path to the RemoteFileServer object
  client.setPath("RemotingWrapper.RemoteFileServer")

  # Create serialization context
  let ctx = newSerializationContext()
  
  # Create method call with ArgsInline flag
  var flags: MessageFlags = {MessageFlag.ArgsInline, MessageFlag.NoContext}
  
  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"
  
  var call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("Close"),
    typeName: newStringValueWithCode(fullTypeName)
  )
  
  # Create the three string arguments
  var args: seq[ValueWithCode] = @[]
  args.add(toValueWithCode(stringValue(serverGUID)))     # Server GUID
  args.add(toValueWithCode(stringValue(databaseGUID)))  # Database GUID
  args.add(toValueWithCode(stringValue(fileName)))      # File name
  
  call.args = args
  
  # Create the message
  let msg = newRemotingMessage(ctx, methodCall = some(call))
  
  # Serialize and send
  let requestData = serializeRemotingMessage(msg, ctx)
  
  echo "Closing file on server..."
  echo "  Server GUID: ", serverGUID
  echo "  Database GUID: ", databaseGUID
  echo "  File Name: ", fileName
  
  let responseData = await client.invoke("Close", fullTypeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Extract the return status
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let status = ret.returnValue.value.int32Val
      if status == 0:
        echo "File closed successfully"
      else:
        echo "Failed to close file with status code: ", status
      return status
  
  raise newException(IOError, "Failed to close file")

proc readFile*(client: NrtpTcpClient, serverGUID: string, databaseGUID: string, 
               fileName: string, filePos: int64, count: uint32): 
               Future[tuple[status: int32, data: seq[byte]]] {.async.} =
  ## Reads a chunk of data from a file on the remote file server
  ## Parameters:
  ##   serverGUID - The server GUID
  ##   databaseGUID - The database GUID
  ##   fileName - The name of the file to read from
  ##   filePos - The position in the file to start reading from
  ##   count - The number of bytes to read
  ## Returns a tuple with:
  ##   status - Status code (0 for success)
  ##   data - The bytes read from the file
  
  # Set the path to the RemoteFileServer object
  client.setPath("RemotingWrapper.RemoteFileServer")

  # Create method call with the required parameters
  let typeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper"
  
  # Create arguments: serverGUID, databaseGUID, fileName, null for ref pData, filePos, count
  let args = @[
    stringValue(serverGUID),     # Server GUID parameter
    stringValue(databaseGUID),   # Database GUID parameter
    stringValue(fileName),       # File name parameter
    PrimitiveValue(kind: ptNull), # Placeholder for ref byte[] pData parameter
    int64Value(filePos),         # nFilePos parameter
    uint32Value(count)           # nCount parameter
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "Read",
    typeName = typeName,
    args = args
  )
  
  #[ echo "Reading file data..."
  echo "  Server GUID: ", serverGUID
  echo "  Database GUID: ", databaseGUID
  echo "  File Name: ", fileName
  echo "  Position: ", filePos
  echo "  Bytes to read: ", count ]#
  
  let responseData = await client.invoke("Read", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value (status code)
    var status: int32 = -1
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      status = ret.returnValue.value.int32Val
      if status != 0:
        echo "Read failed with status code: ", status
        return (status, @[])
    
    # The response should have the byte array in the arguments
    # Based on the method signature, the byte array should be the 4th argument (index 3)
    if MessageFlag.ArgsInline in ret.messageEnum and ret.args.len >= 4:
      # The fourth parameter should be the byte array data
      
      if MessageFlag.ArgsInArray in ret.messageEnum:
        # Data is in the array
        if msg.methodCallArray.len >= 4:
          let dataRef = msg.methodCallArray[3]
          if dataRef.kind == rvReference:
            # Find the referenced byte array in the records
            for record in msg.referencedRecords:
              if record.kind == rvArray:
                let arrayRecord = record.arrayVal.record
                if arrayRecord.kind == rtArraySinglePrimitive:
                  let arrayPrim = arrayRecord.arraySinglePrimitive
                  if arrayPrim.primitiveType == ptByte and arrayPrim.arrayInfo.objectId == dataRef.idRef:
                    var data: seq[byte] = @[]
                    for elem in record.arrayVal.elements:
                      if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
                        data.add(elem.primitiveVal.byteVal)
                    
                    #echo "Successfully read ", data.len, " bytes"
                    return (status, data)
    else:
      # Check if data is in referenced records directly
      for record in msg.referencedRecords:
        if record.kind == rvArray:
          let arrayRecord = record.arrayVal.record
          if arrayRecord.kind == rtArraySinglePrimitive:
            let arrayPrim = arrayRecord.arraySinglePrimitive
            if arrayPrim.primitiveType == ptByte:
              var data: seq[byte] = @[]
              for elem in record.arrayVal.elements:
                if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
                  data.add(elem.primitiveVal.byteVal)
              
              #echo "Successfully read ", data.len, " bytes"
              return (status, data)
    
    echo "Warning: Could not find byte array data in response, but status was success"
    return (status, @[])
  
  raise newException(IOError, "Failed to read file data")


proc downloadFile*(client: NrtpTcpClient, serverGUID: string, databaseGUID: string,
                   fileName: string, outputPath: string, chunkSize: uint32 = 262144'u32): Future[bool] {.async.} =
  ## Downloads a complete file from the remote file server
  ## Parameters:
  ##   serverGUID - The server GUID
  ##   databaseGUID - The database GUID
  ##   fileName - The name of the file to download
  ##   outputPath - The local path where the file should be saved
  ##   chunkSize - The size of chunks to read (default: 128KB)
  ## Returns true if successful, false otherwise
  
  echo "Downloading file: ", fileName, " to ", outputPath
  
  # First, open the file
  let openStatus = await openFile(client, serverGUID, databaseGUID, fileName)
  if openStatus != 0:
    echo "Failed to open file"
    return false

  # Get the file size
  let fileSizeResult = await getFileSize(client, serverGUID, databaseGUID, fileName)
  if fileSizeResult.status != 0:
    echo "Failed to get file size"
    return false
  
  let totalSize = fileSizeResult.fileSize
  echo "Total file size: ", totalSize, " bytes"
  
  let outputDir = outputPath.parentDir()
  if outputDir != "" and not dirExists(outputDir):
    try:
      createDir(outputDir)
      echo "Created directory: ", outputDir
    except OSError as e:
      echo "Failed to create directory: ", outputDir, " - ", e.msg
      discard await closeFile(client, serverGUID, databaseGUID, fileName)
      return false
  
  # Create/open the output file
  var outputFile: File
  if not open(outputFile, outputPath, fmWrite):
    echo "Failed to create output file: ", outputPath
    discard await closeFile(client, serverGUID, databaseGUID, fileName)
    return false
  
  try:
    var position: int64 = 0
    var totalRead: int64 = 0
    
    while position < totalSize:
      let remainingBytes = totalSize - position
      let bytesToRead = if remainingBytes > chunkSize.int64: chunkSize else: remainingBytes.uint32
      
      let readResult = await readFile(client, serverGUID, databaseGUID, fileName, position, bytesToRead)
      if readResult.status != 0:
        echo "\nFailed to read file at position ", position
        return false
      
      if readResult.data.len > 0:
        discard outputFile.writeBuffer(readResult.data[0].unsafeAddr, readResult.data.len)
        totalRead += readResult.data.len
        position += readResult.data.len
        
        let progress = (totalRead.float / totalSize.float) * 100.0
        let shouldUpdate = (totalRead mod (1024 * 1024) < readResult.data.len) or
                          ((progress * 10).int mod 1 == 0 and progress > (totalRead - readResult.data.len).float / totalSize.float * 100.0 * 10) or
                          (totalRead == totalSize)
        
        if shouldUpdate or totalRead == totalSize:
          stdout.write("\rProgress: ", formatFloat(progress, ffDecimal, 2), "% (", 
                      formatFloat(totalRead.float / 1024.0 / 1024.0, ffDecimal, 1), " MB / ",
                      formatFloat(totalSize.float / 1024.0 / 1024.0, ffDecimal, 1), " MB)")
          stdout.flushFile()
      else:
        echo "\nWarning: Read returned 0 bytes at position ", position

  finally:
    outputFile.close()
    
    # Close the file on the server
    let closeStatus = await closeFile(client, serverGUID, databaseGUID, fileName)
    if closeStatus != 0:
      echo "Warning: Failed to close file on server"
  
  return true
