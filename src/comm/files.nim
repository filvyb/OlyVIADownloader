import faststreams/inputs
import asyncdispatch
import strutils

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

  # Create method call with the required parameters
  let typeName = "RemotingWrapper.RemoteFileServer, RemotingWrapper"
  
  # Create arguments including placeholder for ref parameter
  let args = @[
    stringValue(serverURL),      # strServerURL parameter
    stringValue(databaseGUID),   # strDatabaseGUID parameter
    if entryGUID == "": PrimitiveValue(kind: ptNull) else: stringValue(entryGUID),  # strEntryGUID parameter
    PrimitiveValue(kind: ptNull) # Placeholder for ref strFileNames parameter
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "GetFileList",
    typeName = typeName,
    args = args
  )
  
  echo "Getting file list from server..."
  echo "  Server URL: ", serverURL
  echo "  Database GUID: ", databaseGUID
  if entryGUID != "":
    echo "  Entry GUID: ", entryGUID
  
  let responseData = await client.invoke("GetFileList", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Extract the return status
    var status: int32 = -1
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      status = ret.returnValue.value.int32Val
      if status != 1:
        echo "GetFileList failed with status code: ", status
        return (status, @[])
    
    # The response structure based on packet analysis:
    # 1. BinaryMethodReturn with status code (1 = success)
    # 2. ArraySingleObject with 3 items (one for each ref parameter):
    #    - Item 0-2: null values (for the input parameters)
    #    - Item 3: MemberReference to the file names array
    # 3. ArraySingleString containing the actual file names
    
    # Find the file names array in the referenced records
    var fileNames: seq[string] = @[]
    
    # The methodCallArray should contain the reference to the string array
    if msg.methodCallArray.len > 0:
      # Find the reference to the string array (should be the last item)
      let lastItem = msg.methodCallArray[^1]
      if lastItem.kind == rvReference:
        let refId = lastItem.idRef
        
        # Find the referenced array in the records
        for record in msg.referencedRecords:
          if record.kind == rvArray:
            let arrayRecord = record.arrayVal.record
            if arrayRecord.kind == rtArraySingleString:
              let arrayInfo = arrayRecord.arraySingleString.arrayInfo
              if arrayInfo.objectId == refId:
                # Extract the file names
                for elem in record.arrayVal.elements:
                  if elem.kind == rvString:
                    fileNames.add(elem.stringVal.value)
                  # Note: null elements might indicate empty slots
                
                echo "Successfully retrieved ", fileNames.len, " files"
                return (status, fileNames)
    
    # If we didn't find the array through references, it might be directly in referenced records
    for record in msg.referencedRecords:
      if record.kind == rvArray:
        let arrayRecord = record.arrayVal.record
        if arrayRecord.kind == rtArraySingleString:
          # This should be our file names array
          for elem in record.arrayVal.elements:
            if elem.kind == rvString:
              fileNames.add(elem.stringVal.value)
          
          echo "Successfully retrieved ", fileNames.len, " files"
          return (status, fileNames)
    
    echo "Warning: Could not find file names in response, but status was success"
    return (status, @[])
  
  raise newException(IOError, "Failed to get file list")