import faststreams/inputs
import asyncdispatch
import strutils

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums, types, context]
import DotNimRemoting/msnrbf/records/[methodinv, member, class, arrays]

proc testConnection*(client: NrtpTcpClient): Future[bool] {.async.} =
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

proc createSession*(client: NrtpTcpClient): Future[int32] {.async.} =
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

proc getInterfaceVersion*(client: NrtpTcpClient, interfaceId: uint32): Future[tuple[major, minor, micro: int32]] {.async.} =
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

proc dissolveAlias*(client: NrtpTcpClient, alias: string): Future[tuple[status: int32, dbName: string, dbLocation: string, dbmsType: int32]] {.async.} =
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

proc createConnectionObject*(client: NrtpTcpClient, sessionId: int32): Future[int32] {.async.} =
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


proc connectToDatabase*(client: NrtpTcpClient, sessionId: int32, connectionId: int32, 
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

proc disconnect*(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
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

proc destroyConnectionObject*(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
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
  ## Creates query result objects on the server
  ## Returns a sequence of object IDs that can be used for queries
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create serialization context
  let ctx = newSerializationContext()
  
  # Create the array object first (will get ID 3)
  let emptyArrayRecord = ArrayRecord(
    kind: rtArraySingleObject,
    arraySingleObject: ArraySingleObject(
      recordType: rtArraySingleObject,
      arrayInfo: ArrayInfo(
        objectId: 0,  # Will be assigned during serialization
        length: 32    # Initial capacity
      )
    )
  )
  
  # Create null elements for the array
  var arrayElements: seq[RemotingValue]
  for i in 0..<32:
    arrayElements.add(RemotingValue(kind: rvNull))
  
  let arrayValue = RemotingValue(
    kind: rvArray,
    arrayVal: ArrayValue(
      record: emptyArrayRecord,
      elements: arrayElements
    )
  )
  
  # Create the Queue class members with reference to array
  let queueMembers = @[
    RemotingValue(kind: rvReference, idRef: 3),                           # _array (reference to ID 3)
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),        # _head
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),        # _tail
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0)),        # _size
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(200)),      # _growFactor (200 = 2.0 as percentage)
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(0))         # _version
  ]
  
  # Create member type info for System.Collections.Queue
  let memberInfos = @[
    (name: "_array", btype: btObjectArray, addInfo: AdditionalTypeInfo(kind: btObjectArray)),
    (name: "_head", btype: btPrimitive, addInfo: AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32)),
    (name: "_tail", btype: btPrimitive, addInfo: AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32)),
    (name: "_size", btype: btPrimitive, addInfo: AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32)),
    (name: "_growFactor", btype: btPrimitive, addInfo: AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32)),
    (name: "_version", btype: btPrimitive, addInfo: AdditionalTypeInfo(kind: btPrimitive, primitiveType: ptInt32))
  ]
  
  # Create the Queue class record
  let queueClassRecord = systemClassWithMembersAndTypes("System.Collections.Queue", memberInfos)
  
  # Create the Queue RemotingValue (will get ID 2)
  let queueValue = RemotingValue(
    kind: rvClass,
    classVal: ClassValue(
      record: ClassRecord(
        kind: rtSystemClassWithMembersAndTypes,
        systemClassWithMembersAndTypes: queueClassRecord
      ),
      members: queueMembers
    )
  )
  
  # Create method call with ArgsIsArray flag
  var flags: MessageFlags = {MessageFlag.ArgsIsArray, MessageFlag.NoContext}
  
  let call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("CreateQueryResultObjects"),
    typeName: newStringValueWithCode("dbapi.dni.ILLRemoteServer, LLDbRemoting, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
  )
  
  # Create the arguments array with reference to queue
  let argsArray = @[
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(sessionId)),  # Client
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(count)),      # nCount
    RemotingValue(kind: rvReference, idRef: 2)                             # Reference to Queue (ID 2)
  ]
  
  # Create the message with both the queue and array as referenced records
  let msg = newRemotingMessage(ctx, 
    methodCall = some(call), 
    callArray = argsArray,
    refs = @[queueValue, arrayValue]  # Queue gets ID 2, Array gets ID 3
  )
  
  # Serialize and send
  let requestData = serializeRemotingMessage(msg, ctx)
  
  echo "Creating ", count, " query result objects..."
  let responseData = await client.invoke("CreateQueryResultObjects", 
                                       "dbapi.dni.ILLRemoteServer, LLDbRemoting, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", 
                                       false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  
  echo "Failed to create query result objects"
  return @[]
