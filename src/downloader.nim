import faststreams/inputs
import asyncdispatch
import strutils, os

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums, types]
import DotNimRemoting/msnrbf/records/[methodinv, member]

proc testConnection(client: NrtpTcpClient): Future[bool] {.async.} =
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
  
  try:
    await client.connect()
    echo "Connected to server"
    
    # Test connection
    let testResult = await testConnection(client)
    if not testResult:
      echo "Failed to verify connection with server"
      return
    
    let sessionId = await createSession(client)
    echo "Using session ID: ", sessionId
    
    # TODO: Implement authentication
    
    # TODO: Implement file listing
    
    # TODO: Implement file download
    
  except Exception as e:
    echo "Error: ", e.msg
  finally:
    await client.close()
    echo "Connection closed"