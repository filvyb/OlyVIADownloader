import asyncdispatch
import strutils, os

import comm/[initial, db]
import utils

import DotNimRemoting/tcp/client

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
    var queryResultIds = await createQueryResultObjects(client, sessionId, 100)  # Create 100 query result objects
    if queryResultIds.len == 0:
      echo "Failed to create query result objects"
      return
    echo "Created ", queryResultIds.len, " query result objects"
    echo "First query result ID: ", queryResultIds[0]
    echo "Last query result ID: ", queryResultIds[^1]
    
    # Execute some query
    let sqlQuery1 = "SELECT isnull( object_id('vw_DBParameters', 'V'), 0) as CNT"
    #let boostText = """
    #  22 serialization::archive 4 0 0
    #  1 0 0 0 1 -94 -1 0 0 0 1 2 0 0 1 0 0 0 1 5 1 1"""
    #let paramsIn = boostBinToZip(boostText)
    let paramsIn1 = @[
      0x50'u8, 0x4b, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00, 0xcc, 0x84, 0x32, 0x59, 0x97, 0xe4,
      0x41, 0xf7, 0x79, 0x00, 0x00, 0x00, 0xda, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x73, 0x65,
      0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69, 0x6e, 0x6d, 0x8e, 0xb1, 0x0a,
      0x83, 0x40, 0x10, 0x05, 0xad, 0x05, 0x3f, 0x47, 0x49, 0x93, 0x62, 0x4e, 0x8f, 0x53, 0xc8, 0x15,
      0x5b, 0x58, 0x68, 0x67, 0x12, 0xb8, 0x14, 0x81, 0xa4, 0x08, 0x5c, 0xee, 0xef, 0x93, 0x55, 0x41,
      0x03, 0xa9, 0x96, 0x99, 0x7d, 0x6f, 0x59, 0x1f, 0x48, 0xd4, 0x70, 0x89, 0xe6, 0x4e, 0xdb, 0x31,
      0x89, 0xb9, 0xe1, 0xa2, 0xce, 0x0a, 0x67, 0xb9, 0x62, 0x9e, 0xb8, 0x03, 0xe7, 0x40, 0x45, 0xf3,
      0x60, 0x10, 0x93, 0x70, 0x9e, 0x49, 0x7d, 0x3b, 0x30, 0x0a, 0x81, 0x46, 0xe8, 0x20, 0x16, 0xb9,
      0x5e, 0xf2, 0xa8, 0xb1, 0xb3, 0xd9, 0x18, 0xe5, 0xb7, 0xf2, 0x49, 0x28, 0xd7, 0xc6, 0x6b, 0x9f,
      0xfb, 0xcd, 0x5b, 0x8a, 0xfc, 0x6b, 0xd2, 0xbf, 0x0b, 0x3b, 0x5e, 0x1a, 0xf3, 0x0f, 0xfd, 0xb6,
      0x17, 0x8e, 0x59, 0xf6, 0x01, 0x50, 0x4b, 0x01, 0x02, 0x2e, 0x00, 0x14, 0x00, 0x00, 0x00, 0x08,
      0x00, 0xcc, 0x84, 0x32, 0x59, 0x97, 0xe4, 0x41, 0xf7, 0x79, 0x00, 0x00, 0x00, 0xda, 0x00, 0x00,
      0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0xb4, 0x81, 0x00,
      0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69,
      0x6e, 0x50, 0x4b, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x3c, 0x00, 0x00,
      0x00, 0xa5, 0x00, 0x00, 0x00, 0x00, 0x00
    ] 
    let sqlQuery1Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery1],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[0],  # Use first query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery1Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery1, " (status code: ", sqlQuery1Res.status, ")"
      return
    
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
