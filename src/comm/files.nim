import asyncdispatch
import strutils, options
import os

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums]
import DotNimRemoting/msnrbf/records/methodinv

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

  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"

  let args = @[
    toRemotingValue(serverURL),                                      # strServerURL
    toRemotingValue(databaseGUID),                                   # strDatabaseGUID
    if entryGUID == "": nullValue() else: toRemotingValue(entryGUID) # strEntryGUID
  ]

  let requestData = createMethodCallRequest(
    methodName = "GetFileList",
    typeName = fullTypeName,
    args = args
  )

  echo "Getting file list from server..."
  echo "  Server URL: ", serverURL
  echo "  Database GUID: ", databaseGUID
  if entryGUID != "":
    echo "  Entry GUID: ", entryGUID

  let responseData = await client.invoke("GetFileList", fullTypeName, false, requestData)

  # Parse response
  let responseMsg = deserializeRemotingMessage(responseData)

  if responseMsg.methodReturn.isSome:
    # Resolves references and raises RemoteException on .NET exception replies
    let retVal = returnValueOf(responseMsg)

    var status: int32 = -1
    if retVal.kind == rvPrimitive and retVal.primitiveVal.kind == ptInt32:
      status = retVal.getInt32
      if status != 1:
        echo "GetFileList failed with status code: ", status
        return (status, @[])

    # Response layout: status as return value, args echoed back with the
    # file names as a string array in the third argument position
    var fileNames: seq[string] = @[]

    if responseMsg.methodCallArray.len >= 3:
      let arrItem = responseMsg.methodCallArray[2]
      if arrItem != nil and arrItem.kind == rvArray:
        for elem in arrItem.elements:
          if elem.kind == rvString:
            fileNames.add(elem.getString)

        echo "Successfully retrieved ", fileNames.len, " files"
        return (status, fileNames)

    # Fallback: look for a string array among the referenced records
    for record in responseMsg.referencedRecords:
      if record.kind == rvArray and record.arrayVal.record.kind == rtArraySingleString:
        for elem in record.arrayVal.elements:
          if elem.kind == rvString:
            fileNames.add(elem.getString)

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

  echo "Checking if image is valid - ID: ", imageId
  let ret = await client.call("IsValid", typeName, imageId, databaseGuid)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    if ret.getInt32 == 1:
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

  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"

  echo "Opening file on server..."
  echo "  Server URL: ", serverURL
  echo "  Database GUID: ", databaseGUID
  echo "  File Name: ", fileName
  echo "  Access Mode: 0x", accessMode.toHex()

  let ret = await client.call("Open", fullTypeName,
                              serverURL, databaseGUID, fileName, accessMode)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let status = ret.getInt32
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
    toRemotingValue(serverGUID),  # strServer parameter
    toRemotingValue(entryGUID),   # strEntryGUID parameter
    toRemotingValue(fileName),    # strFileName parameter
    toRemotingValue(0'i64)        # Placeholder for ref strFileSize parameter
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
  let msg = deserializeRemotingMessage(responseData)

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

  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"

  echo "Closing file on server..."
  echo "  Server GUID: ", serverGUID
  echo "  Database GUID: ", databaseGUID
  echo "  File Name: ", fileName

  let ret = await client.call("Close", fullTypeName, serverGUID, databaseGUID, fileName)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let status = ret.getInt32
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
    toRemotingValue(serverGUID),   # Server GUID parameter
    toRemotingValue(databaseGUID), # Database GUID parameter
    toRemotingValue(fileName),     # File name parameter
    nullValue(),                   # Placeholder for ref byte[] pData parameter
    toRemotingValue(filePos),      # nFilePos parameter
    toRemotingValue(count)         # nCount parameter
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
  let msg = deserializeRemotingMessage(responseData)

  if msg.methodReturn.isSome:
    # Resolves references and raises RemoteException on .NET exception replies
    let retVal = returnValueOf(msg)

    var status: int32 = -1
    if retVal.kind == rvPrimitive and retVal.primitiveVal.kind == ptInt32:
      status = retVal.getInt32
      if status != 0:
        echo "Read failed with status code: ", status
        return (status, @[])

    # The byte array comes back in the fourth argument position (ref pData)
    if msg.methodCallArray.len >= 4:
      let dataItem = msg.methodCallArray[3]
      if dataItem != nil and dataItem.kind == rvArray:
        var data: seq[byte] = @[]
        for elem in dataItem.elements:
          if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
            data.add(elem.getByte)

        #echo "Successfully read ", data.len, " bytes"
        return (status, data)

    # Fallback: look for a byte array among the referenced records
    for record in msg.referencedRecords:
      if record.kind == rvArray and record.arrayVal.record.kind == rtArraySinglePrimitive and
         record.arrayVal.record.arraySinglePrimitive.primitiveType == ptByte:
        var data: seq[byte] = @[]
        for elem in record.arrayVal.elements:
          if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
            data.add(elem.getByte)

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
  
  if fileExists(outputPath):
    let existingSize = os.getFileSize(outputPath)
    if existingSize == totalSize:
      echo "File already exists with correct size: ", outputPath
      discard await closeFile(client, serverGUID, databaseGUID, fileName)
      return false
    else:
      echo "File exists but size mismatch (existing: ", existingSize, " bytes, expected: ", totalSize, " bytes). Overwriting..."

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

proc downloadFilePipelined*(client: NrtpTcpClient, serverGUID: string, databaseGUID: string,
                           fileName: string, outputPath: string, chunkSize: uint32 = 262144'u32): Future[bool] {.async.} =
  ## Downloads a file with pipelined reading - reads next chunk while writing current chunk
  
  echo "Downloading file: ", fileName, " to ", outputPath
  
  let openStatus = await openFile(client, serverGUID, databaseGUID, fileName)
  if openStatus != 0:
    echo "Failed to open file"
    return false

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
  
  if fileExists(outputPath):
    let existingSize = os.getFileSize(outputPath)
    if existingSize == totalSize:
      echo "File already exists with correct size: ", outputPath
      discard await closeFile(client, serverGUID, databaseGUID, fileName)
      return false
    else:
      echo "File exists but size mismatch. Overwriting..."

  var outputFile: File
  if not open(outputFile, outputPath, fmWrite):
    echo "Failed to create output file: ", outputPath
    discard await closeFile(client, serverGUID, databaseGUID, fileName)
    return false
  
  try:
    var position: int64 = 0
    var totalRead: int64 = 0
    var currentReadFuture: Future[tuple[status: int32, data: seq[byte]]]
    var pendingData: seq[byte]
    var hasPendingData = false
    
    # Start first read
    if position < totalSize:
      let remainingBytes = totalSize - position
      let bytesToRead = if remainingBytes > chunkSize.int64: chunkSize else: remainingBytes.uint32
      currentReadFuture = readFile(client, serverGUID, databaseGUID, fileName, position, bytesToRead)
      position += bytesToRead.int64
    
    while totalRead < totalSize:
      # Wait for current read to complete
      let readResult = await currentReadFuture
      if readResult.status != 0:
        echo "\nFailed to read file at position ", position - readResult.data.len
        return false
      
      # Start next read immediately (if more data to read)
      var nextReadFuture: Future[tuple[status: int32, data: seq[byte]]]
      var nextReadStarted = false
      
      if position < totalSize:
        let remainingBytes = totalSize - position
        let bytesToRead = if remainingBytes > chunkSize.int64: chunkSize else: remainingBytes.uint32
        nextReadFuture = readFile(client, serverGUID, databaseGUID, fileName, position, bytesToRead)
        position += bytesToRead.int64
        nextReadStarted = true
      
      if readResult.data.len > 0:
        discard outputFile.writeBuffer(readResult.data[0].unsafeAddr, readResult.data.len)
        totalRead += readResult.data.len
        
        let progress = (totalRead.float / totalSize.float) * 100.0
        stdout.write("\rProgress: ", formatFloat(progress, ffDecimal, 2), "% (", 
                    formatFloat(totalRead.float / 1024.0 / 1024.0, ffDecimal, 1), " MB / ",
                    formatFloat(totalSize.float / 1024.0 / 1024.0, ffDecimal, 1), " MB)")
        stdout.flushFile()
      
      if nextReadStarted:
        currentReadFuture = nextReadFuture

  finally:
    outputFile.close()
    
    let closeStatus = await closeFile(client, serverGUID, databaseGUID, fileName)
    if closeStatus != 0:
      echo "Warning: Failed to close file on server"
  
  return true
