import faststreams/inputs
import asyncdispatch
import strutils, os

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums, types, context]
import DotNimRemoting/msnrbf/records/[methodinv, member, class, arrays, serialization]

type
  Queue* = ref object
    ## Simple representation of System.Collections.Queue
    array*: seq[int32]
    head*: int32
    tail*: int32
    size*: int32
    growFactor*: int32
    version*: int32

proc testConnection(client: NrtpTcpClient): Future[bool] {.async.} =
  ## Tests the connection to the server by invoking the "Test" method
  # Create a "Test" method call to verify connectivity
  let typeName = "dbapi.dni.ILLRemoteServerAccess, LLDbRemoting"
  let requestData = createMethodCallRequest(
    methodName = "Test",
    typeName = typeName
  )
  
  echo "Testing connection to server..."
  let responseData = await client.invoke("Test", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptBoolean:
      echo "Connection test successful!"
      return ret.returnValue.value.boolVal
  
  echo "Connection test failed"
  return false

proc createSession(client: NrtpTcpClient): Future[int32] {.async.} =
  ## Creates a new session on the server

  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "Create" method call to create a session on the server
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  let requestData = createMethodCallRequest(
    methodName = "Create",
    typeName = typeName
  )
  
  echo "Creating session..."
  let responseData = await client.invoke("Create", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let sessionId = ret.returnValue.value.int32Val
      echo "Session created with ID: ", sessionId
      return sessionId
  
  echo "Failed to create session"
  raise newException(IOError, "Failed to create session")

proc getInterfaceVersion(client: NrtpTcpClient, interfaceId: uint32): Future[tuple[major, minor, micro: int32]] {.async.} =
  ## Retrieves the interface version from the server
  ## InterfaceId 2 represents the ILLRemoteServer interface
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create method call with the required parameters
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: interfaceId and placeholders for ref parameters
  let args = @[
    uint32Value(interfaceId),  # Interface ID (uint32)
    int32Value(-1),  # Placeholder for ref Major parameter
    int32Value(-1),  # Placeholder for ref Minor parameter
    int32Value(-1)   # Placeholder for ref Micro parameter
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "GetInterfaceVersion",
    typeName = typeName,
    args = args
  )
  
  echo "Getting interface version..."
  let responseData = await client.invoke("GetInterfaceVersion", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum:
      let returnCode = ret.returnValue.value.int32Val
      if returnCode != 0:
        raise newException(IOError, "GetInterfaceVersion failed with code: " & $returnCode)
    
    # Check if we have arguments in the response
    if MessageFlag.ArgsInline in ret.messageEnum and ret.args.len >= 3:
      # Extract version components from output arguments
      let major = ret.args[1].value.int32Val
      let minor = ret.args[2].value.int32Val 
      let micro = ret.args[3].value.int32Val
      
      echo "Interface version: ", major, ".", minor, ".", micro
      return (major, minor, micro)
  
  raise newException(IOError, "Failed to get interface version")

proc dissolveAlias(client: NrtpTcpClient, alias: string): Future[tuple[status: int32, dbName: string, dbLocation: string, dbmsType: int32]] {.async.} =
  ## Resolves a database alias to its actual database connection information
  ## Returns a tuple with the status code, database name, database location, and DBMS type
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create method call with the required parameters
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: alias and placeholders for ref parameters
  let args = @[
    stringValue(alias),    # Input alias parameter
    PrimitiveValue(kind: ptNull),       # Placeholder for ref database name
    PrimitiveValue(kind: ptNull),       # Placeholder for ref database location
    int32Value(0)          # Placeholder for ref DBMS type
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "DissolveAlias",
    typeName = typeName,
    args = args
  )
  
  echo "Resolving alias: ", alias
  let responseData = await client.invoke("DissolveAlias", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum:
      let statusCode = ret.returnValue.value.int32Val
      
      # Check if we have arguments in the response
      if MessageFlag.ArgsInline in ret.messageEnum and ret.args.len >= 3:
        # Extract the reference parameters
        # Note: The first parameter is the unchanged input alias
        let dbName = ret.args[1].value.stringVal.value      # First output is database name
        let dbLocation = ret.args[2].value.stringVal.value  # Second output is database location
        let dbmsType = ret.args[3].value.int32Val           # Third output is DBMS type
        
        echo "Alias resolved: database=", dbName, ", location=", dbLocation, ", type=", dbmsType
        return (statusCode, dbName, dbLocation, dbmsType)
      
      # If we don't have enough arguments, just return the status code
      return (statusCode, "", "", int32(0))
  
  raise newException(IOError, "Failed to resolve alias")

proc createConnectionObject(client: NrtpTcpClient, sessionId: int32): Future[int32] {.async.} =
  ## Creates a connection object using the provided session ID
  ## Returns the connection object ID that can be used for database operations
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "CreateConnectionObject" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create argument: sessionId
  let args = @[
    int32Value(sessionId)  # Session ID (int32)
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "CreateConnectionObject",
    typeName = typeName,
    args = args
  )
  
  echo "Creating connection object for session ID: ", sessionId
  let responseData = await client.invoke("CreateConnectionObject", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let connectionId = ret.returnValue.value.int32Val
      echo "Connection object created with ID: ", connectionId
      return connectionId
  
  echo "Failed to create connection object"
  raise newException(IOError, "Failed to create connection object")


proc connectToDatabase(client: NrtpTcpClient, sessionId: int32, connectionId: int32, 
                      dbName: string, dbLocation: string, dbmsType: int32,
                      username: string, password: string, domain: string = ""): Future[int32] {.async.} =
  ## Connects to the database using the provided connection details
  ## Returns the status code (0 for success)
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "ConnectEx" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments according to the ConnectEx method signature
  var args: seq[PrimitiveValue] = @[
    int32Value(sessionId),        # Client/Session ID
    int32Value(connectionId),     # Connection ID
    if dbName == "": PrimitiveValue(kind: ptNull) else: stringValue(dbName),  # DatabaseName (NULL if empty)
    stringValue(dbLocation),      # DatabaseLocation
    int32Value(dbmsType),         # DBMSType
    stringValue(username),        # User
    stringValue(password),        # Password
    if domain == "": PrimitiveValue(kind: ptNull) else: stringValue(domain)   # Domain (NULL if empty)
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "ConnectEx",
    typeName = typeName,
    args = args
  )
  
  echo "Connecting to database at: ", dbLocation
  let responseData = await client.invoke("ConnectEx", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      if statusCode == 0:
        echo "Successfully connected to database"
      else:
        echo "Failed to connect to database, error code: ", statusCode
      return statusCode
  
  echo "Failed to connect to database, unexpected response"
  raise newException(IOError, "Failed to connect to database")

proc disconnect(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Gracefully disconnects from the server
  ## Parameters:
  ##   sessionId - The session ID (Client parameter in the interface)
  ##   connectionId - The connection ID (Connection parameter in the interface)
  ## Returns the status code (0 for success)
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "Disconnect" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: sessionId and connectionId
  let args = @[
    int32Value(sessionId),    # Client/Session ID
    int32Value(connectionId)  # Connection ID
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "Disconnect",
    typeName = typeName,
    args = args
  )
  
  echo "Disconnecting session ID: ", sessionId, ", connection ID: ", connectionId
  let responseData = await client.invoke("Disconnect", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      if statusCode == 0:
        echo "Successfully disconnected from database"
      else:
        echo "Failed to disconnect from database, error code: ", statusCode
      return statusCode
  
  echo "Failed to disconnect from database, unexpected response"
  raise newException(IOError, "Failed to disconnect from database")

proc destroyConnectionObject(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Destroys a connection object on the server
  ## Parameters:
  ##   sessionId - The session ID (Client parameter in the interface)
  ##   connectionId - The connection ID (Connection parameter in the interface)
  ## Returns the status code (0 for success)
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "DestroyConnectionObject" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: sessionId and connectionId
  let args = @[
    int32Value(sessionId),    # Client/Session ID
    int32Value(connectionId)  # Connection ID
  ]
  
  # Create request
  let requestData = createMethodCallRequest(
    methodName = "DestroyConnectionObject",
    typeName = typeName,
    args = args
  )
  
  echo "Destroying connection object - session ID: ", sessionId, ", connection ID: ", connectionId
  let responseData = await client.invoke("DestroyConnectionObject", typeName, false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let msg = readRemotingMessage(input)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      if statusCode == 0:
        echo "Successfully destroyed connection object"
      else:
        echo "Failed to destroy connection object, error code: ", statusCode
      return statusCode
  
  echo "Failed to destroy connection object, unexpected response"
  raise newException(IOError, "Failed to destroy connection object")

proc createQueryResultObjects*(client: NrtpTcpClient, sessionId: int32, count: int32): Future[seq[int32]] {.async.} =
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null"
  let ctx = newSerializationContext() # Start IDs at 1

  # --- Define Objects (Headers first, then full values) ---
  # We need to control the ID assignment order: 1, 2, 3

  # --- 1. Call Array Object (Target ObjectId = 1) ---
  let callArrayLength = 3
  # Create the *header* record first
  let callArrayRecord = ArrayRecord(
      kind: rtArraySingleObject,
      arraySingleObject: ArraySingleObject(
          recordType: rtArraySingleObject,
          arrayInfo: ArrayInfo(length: callArrayLength.int32) # ID assigned next
      )
  )
  # Assign ID 1 explicitly for the Call Array (Root Object)
  let callArrayObjectId = ctx.assignIdForPointer(cast[pointer](addr callArrayRecord.arraySingleObject))
  if callArrayObjectId != 1:
      raise newException(ValueError, "Call Array did not get ID 1, got " & $callArrayObjectId)
  callArrayRecord.arraySingleObject.arrayInfo.objectId = callArrayObjectId # Set ID in record
  # Create the ReferenceableRecord wrapper needed for newRemotingMessage
  let callArrayRefRecord = some(ReferenceableRecord(kind: rtArraySingleObject, arrayRecord: callArrayRecord))

  # --- 2. Queue Object (Target ObjectId = 2) ---
  let queueMemberNames = @[
    LengthPrefixedString(value: "_array"),
    LengthPrefixedString(value: "_head"),
    LengthPrefixedString(value: "_tail"),
    LengthPrefixedString(value: "_size"),
    LengthPrefixedString(value: "_growFactor"),
    LengthPrefixedString(value: "_version")
  ]

  # *** FIX: Manually define MemberTypeInfo ***
  let queueMemberTypeInfo = MemberTypeInfo(
      binaryTypes: @[btObjectArray, btPrimitive, btPrimitive, btPrimitive, btPrimitive, btPrimitive],
      additionalInfos: @[
        AdditionalTypeInfo(kind: btObjectArray), # Correct type for _array
        AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32),
        AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32),
        AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32),
        AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32),
        AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32)
      ]
  )
  # Create the *header* record using the manual MemberTypeInfo
  let queueClassRecord = ClassRecord(
    kind: rtSystemClassWithMembersAndTypes,
    systemClassWithMembersAndTypes: SystemClassWithMembersAndTypes(
      recordType: rtSystemClassWithMembersAndTypes,
      classInfo: ClassInfo(
        # objectId assigned later by context
        name: LengthPrefixedString(value: "System.Collections.Queue"),
        memberCount: 6,
        memberNames: queueMemberNames
      ),
      memberTypeInfo: queueMemberTypeInfo # Use the manually defined info
    )
  )
  # Create the full RemotingValue for the Queue (using placeholder ID 0 for now)
  var queueRemotingValue = RemotingValue(
    kind: rvClass,
    classVal: ClassValue(
      record: queueClassRecord, # Link the header record
      members: @[ # Members defined later after IDs are known
        RemotingValue(kind: rvReference, idRef: 0), # Placeholder ID for internal array
        RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),  # _head
        RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),  # _tail
        RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),  # _size
        RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(200)), # _growFactor
        RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0))   # _version
      ]
    )
  )
  # Assign ID 2 explicitly for the Queue
  let queueObjectId = ctx.assignIdForPointer(cast[pointer](addr queueRemotingValue.classVal))
  if queueObjectId != 2:
      raise newException(ValueError, "Queue did not get ID 2, got " & $queueObjectId)
  queueRemotingValue.classVal.record.systemClassWithMembersAndTypes.classInfo.objectId = queueObjectId # Set ID in header record


  # --- 3. Internal Array Object (Target ObjectId = 3) ---
  let internalArrayLength = 32
  # Create the *header* record first
  let internalArrayRecord = ArrayRecord(
    kind: rtArraySingleObject,
    arraySingleObject: ArraySingleObject(
      recordType: rtArraySingleObject,
      arrayInfo: ArrayInfo(length: internalArrayLength.int32) # ID assigned later by context
    )
  )
  # Create the *elements* for the internal array
  var internalArrayElements = newSeq[RemotingValue](internalArrayLength)
  for i in 0..<internalArrayLength:
    internalArrayElements[i] = RemotingValue(kind: rvNull) # Fill with nulls
  # Create the full RemotingValue for the internal array
  let internalArrayRemotingValue = RemotingValue(
    kind: rvArray,
    arrayVal: ArrayValue(
      record: internalArrayRecord, # Link the header record
      elements: internalArrayElements # Include the elements
    )
  )
  # Assign ID 3 explicitly for the Internal Array
  let internalArrayObjectId = ctx.assignIdForPointer(cast[pointer](addr internalArrayRemotingValue.arrayVal))
  if internalArrayObjectId != 3:
      raise newException(ValueError, "Internal Array did not get ID 3, got " & $internalArrayObjectId)
  internalArrayRemotingValue.arrayVal.record.arraySingleObject.arrayInfo.objectId = internalArrayObjectId # Set ID in header record

  # --- Update Placeholders with Real IDs ---
  # Update the reference in the queue's members list
  queueRemotingValue.classVal.members[0] = RemotingValue(kind: rvReference, idRef: internalArrayObjectId) # Update to actual ID 3

  # --- Define the Call Array Arguments ---
  let callArrayArgs: seq[RemotingValue] = @[
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(sessionId)), # Arg 1
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(count)),     # Arg 2
    RemotingValue(kind: rvReference, idRef: queueObjectId) # Arg 3 (Reference to Queue ID 2)
  ]

  # --- Define the referenced values list for the message ---
  # These are the *full* RemotingValue objects that need to be defined after the call array's arguments.
  # Their headers have IDs 2 and 3 assigned above.
  # The order matters for serialization if the library fix isn't applied, but should ideally not matter.
  let referencedVals = @[
    queueRemotingValue,         # The full Queue object (ID 2)
    internalArrayRemotingValue  # The full internal array object (ID 3)
  ]

  # --- Create the BinaryMethodCall record ---
  # *** Use ArgsIsArray flag ***
  let flags: MessageFlags = {MessageFlag.ArgsIsArray, MessageFlag.NoContext}
  var call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("CreateQueryResultObjects"),
    typeName: newStringValueWithCode(typeName)
    # Args field is NOT used with ArgsIsArray
  )

  # --- Create the RemotingMessage ---
  # Note: newRemotingMessage needs to handle ArgsIsArray correctly.
  # It should use callArrayRefRecord as the root (ID 1) and write
  # callArrayArgs immediately after its header, followed by referencedVals.
  var msg = newRemotingMessage(
    ctx = ctx,
    methodCall = some(call),
    callArray = callArrayArgs,         # Arguments that go *inside* the call array object
    referencedVals = referencedVals,   # Objects defined *after* the call array arguments
    libraries = @[]
    # callArrayRecord is now passed explicitly below
  )
  # Explicitly set the root record for ArgsIsArray case
  msg.callArrayRecord = callArrayRefRecord
  # Ensure header RootId points to the call array
  msg.header.rootId = callArrayObjectId # Should be 1

  # --- Serialize and Send ---
  let requestData = serializeRemotingMessage(msg, ctx) # Use the context

  echo "Generated Request Hex:"
  var hexStr = ""
  for b in requestData: hexStr &= b.toHex(2)
  echo hexStr

  # Compare hexStr with the target packet hex dump

  echo "Creating query result objects: count=", count
  let responseData = await client.invoke("CreateQueryResultObjects", typeName, false, requestData)
  
  # Parse the response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  var resultItems: seq[int32] = @[]
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Check for return value (should be 0 for success)
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      if statusCode != 0:
        echo "Warning: CreateQueryResultObjects returned status code: ", statusCode
    
    # Now process the queue object from the response
    # The queue is passed as reference, so we need to extract its updated state
    if responseMsg.methodCallArray.len > 0:
      # Second item in array should be our updated Queue
      let queueRef = responseMsg.methodCallArray[1]
      
      if queueRef.kind == rvReference:
        let refId = queueRef.idRef
        
        # Find the referenced object in the records
        for record in responseMsg.referencedRecords:
          if record.kind == rtSystemClassWithMembersAndTypes and 
             record.classRecord.systemClassWithMembersAndTypes.classInfo.objectId == refId:
            
            let queue = record.classRecord.systemClassWithMembersAndTypes
            
            # Extract size of queue
            let sizeIdx = 3  # _size is the 4th member (index 3)
            if queue.classInfo.memberNames[sizeIdx].value == "_size":
              if responseMsg.methodCallArray.len > 2:
                let queueObj = responseMsg.methodCallArray[2]
                if queueObj.kind == rvClass:
                  let size = queueObj.classVal.members[sizeIdx].primitiveVal.int32Val
                  
                  # Look for the array containing the items
                  let arrayId = queueObj.classVal.members[0].idRef
                  
                  for rec in responseMsg.referencedRecords:
                    if rec.kind == rtArraySingleObject and 
                       rec.arrayRecord.arraySingleObject.arrayInfo.objectId == arrayId:
                       
                      # Found the array - extract items
                      let items = rec.arrayRecord.arraySingleObject.arrayInfo.length
                      
                      # Now find the actual array value with elements
                      for val in responseMsg.methodCallArray:
                        if val.kind == rvArray and val.arrayVal.record.kind == rtArraySingleObject and
                           val.arrayVal.record.arraySingleObject.arrayInfo.objectId == arrayId:
                           
                          # Extract the actual elements
                          for i in 0..<size:
                            let idx = (i + queueObj.classVal.members[1].primitiveVal.int32Val) mod items
                            if idx < val.arrayVal.elements.len:
                              let elem = val.arrayVal.elements[idx]
                              if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptInt32:
                                resultItems.add(elem.primitiveVal.int32Val)
  
  echo "Retrieved ", resultItems.len, " query result objects"
  return resultItems

proc downloader*(address: string, port: int, username, password, database, directory, file: string) {.async.} =
  echo "Downloading file"
  echo "Address: ", address
  echo "Port: ", port
  echo "Username: ", username
  echo "Password: ", password
  echo "Database: ", database
  echo "Directory: ", directory
  echo "File: ", file
  
  # Create directory if it doesn't exist
  if not dirExists(directory):
    createDir(directory)
  
  # Create client and connect to server
  let serverUri = "tcp://" & address & ":" & $port & "/" & "LLRemoteServerAccess"
  let client = newNrtpTcpClient(serverUri)

  var sessionId: int32 = 0
  var connectionId: int32 = 0
  
  try:
    await client.connect()
    echo "Connected to server"
    
    # Test connection
    let testResult = await testConnection(client)
    if not testResult:
      echo "Failed to verify connection with server"
      return
    
    sessionId = await createSession(client)
    echo "Using session ID: ", sessionId
    
    # Get interface version
    let apiversion = await getInterfaceVersion(client, 2)  # 2 is the ILLRemoteServer interface ID
    echo "Server API version: ", apiversion.major, ".", apiversion.minor, ".", apiversion.micro
    
    # Resolve database alias
    let aliasInfo = await dissolveAlias(client, database)
    if aliasInfo.status != 0:
      echo "Failed to resolve database alias: ", database, " (status code: ", aliasInfo.status, ")"
      return
    
    echo "Database details:"
    echo "  Name: ", aliasInfo.dbName
    echo "  Location: ", aliasInfo.dbLocation
    echo "  DBMS Type: ", aliasInfo.dbmsType

    # Create connection object
    connectionId = await createConnectionObject(client, sessionId)
    echo "Using connection ID: ", connectionId

    # Connect to database
    let connectStatus = await connectToDatabase(client, sessionId, connectionId, 
                                        aliasInfo.dbName, aliasInfo.dbLocation, aliasInfo.dbmsType,
                                        username, password)
    
    if connectStatus != 0:
      echo "Failed to connect to database"
      return
    
    # Create query result objects
    let queryResultObjs = await createQueryResultObjects(client, sessionId, 100)
    # TODO: Implement authentication
    
    # TODO: Implement file listing
    
    # TODO: Implement file download
    
  except Exception as e:
    echo "Error: ", e.msg
  finally:
    # Gracefully disconnect if we have valid session and connection IDs
    if sessionId != 0 and connectionId != 0:
      try:
        let disconnectStatus = await disconnect(client, sessionId, connectionId)
        if disconnectStatus != 0:
          echo "Warning: Disconnect returned non-zero status: ", disconnectStatus
          
        # Destroy the connection object after disconnecting
        let destroyStatus = await destroyConnectionObject(client, sessionId, connectionId)
        if destroyStatus != 0:
          echo "Warning: DestroyConnectionObject returned non-zero status: ", destroyStatus
      except Exception as e:
        echo "Error during cleanup: ", e.msg
    
    await client.close()
    echo "Connection closed"
