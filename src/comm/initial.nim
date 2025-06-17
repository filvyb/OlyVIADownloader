import faststreams/inputs
import asyncdispatch

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums]
import DotNimRemoting/msnrbf/records/[methodinv, member]

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
