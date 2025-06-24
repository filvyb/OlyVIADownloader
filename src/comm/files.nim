import faststreams/inputs
import asyncdispatch
import strutils, options

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
  
  # Create method call with ArgsInline flag (exactly 3 arguments as shown in packet)
  var flags: MessageFlags = {MessageFlag.ArgsInline, MessageFlag.NoContext}
  
  # Create the exact type name as shown in the packet (Version 1.1.0.0)
  let fullTypeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper, Version=1.1.0.0, Culture=neutral, PublicKeyToken=null"
  
  var call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("GetFileList"),
    typeName: newStringValueWithCode(fullTypeName)
  )
  
  # Create exactly 3 arguments as shown in the packet
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

  # Create an "IsValid" method call
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
