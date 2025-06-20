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

    echo sqlQuery1Res.results

    var maxBufferedRows = await getMaxBufferedRowCount(client, sessionId, queryResultIds[0])

    let sqlQuery2 = "SELECT vw_DBParameters.[Parameter], vw_DBParameters.[Value] FROM [dbo].[vw_DBParameters]"

    let sqlQuery2Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery2],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[1],  # Use second query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery2Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery2, " (status code: ", sqlQuery2Res.status, ")"
      return

    echo sqlQuery2Res.results

    if not (await isConnected(client, sessionId, connectionId)):
      echo "Connection is not active, cannot proceed with query results"
      return

    let sqlQuery3 = "select isnull(DB_ID('" & database & "'), 0) as db_cnt"
    # Getting the database ID, I assume
    let sqlQuery3Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery3],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[2],  # Use third query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery3Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery3, " (status code: ", sqlQuery3Res.status, ")"
      return

    echo sqlQuery3Res.results

    #maybe
    #maxBufferedRows = await getMaxBufferedRowCount(client, sessionId, queryResultIds[2])

    let sqlQuery4 = "SELECT isnull( object_id('vw_GetDbVersion', 'V'), 0) as CNT"

    let sqlQuery4Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery4],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[3],  # Use fourth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery4Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery4, " (status code: ", sqlQuery4Res.status, ")"
      return

    echo sqlQuery4Res.results

    #maybe
    #maxBufferedRows = await getMaxBufferedRowCount(client, sessionId, queryResultIds[3])

    let sqlQuery5 = """CREATE TABLE [dbo].[#tb_ProcParams] (
                    [id_Param] bigint NOT NULL,
                    [type_Param] nvarchar (1) NOT NULL,
                    [to_XML] int NULL,
                    [int_value] bigint NULL,
                    [string_value] nvarchar (4000) NULL,
                    [date_value] datetime NULL,
                    [memo_value] nvarchar(max) NULL,
                    [dbl_value] float NULL,
                    [bin_value] image NULL,
                    PRIMARY KEY ([id_Param])
                    )"""
    
    let sqlQuery5Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery5],        # SQL commands array
      1,                  # SqlCommandType
      5,                 # SqlCommandSubType  
      queryResultIds[4],  # Use fifth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery5Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery5, " (status code: ", sqlQuery5Res.status, ")"
      return

    echo sqlQuery5Res.results

    let sqlQuery6 = "[dbo].[sis_GetUserRightsMode]"

    let sqlQuery6Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery6],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[5],  # Use sixth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery6Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery6, " (status code: ", sqlQuery6Res.status, ")"
      return

    # is supposed to be empty, no idea why
    echo sqlQuery6Res.results

    let sqlQuery7 = "SELECT SUSER_SNAME() AS SNAME"

    let sqlQuery7Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery7],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[6],  # Use seventh query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery7Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery7, " (status code: ", sqlQuery7Res.status, ")"
      return

    echo sqlQuery7Res.results

    #maybe
    #maxBufferedRows = await getMaxBufferedRowCount(client, sessionId, queryResultIds[6])

    let sqlQuery8 = """SELECT vw_GetDbVersion.[id_Version], vw_GetDbVersion.[Major], 
                          vw_GetDbVersion.[Minor], vw_GetDbVersion.[Micro], 
                          vw_GetDbVersion.[Version_Comment], vw_GetDbVersion.[Identifier] 
                          FROM [dbo].[vw_GetDbVersion]"""

    let sqlQuery8Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery8],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[7],  # Use eighth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery8Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery8, " (status code: ", sqlQuery8Res.status, ")"
      return

    echo sqlQuery8Res.results

    let paramsIn9 = @[
      0x50'u8, 0x4b, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00, 0xcc, 0x84, 0x32, 0x59, 0x4d, 0xab,
      0x01, 0xdc, 0x9d, 0x00, 0x00, 0x00, 0x30, 0x01, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x73, 0x65,
      0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69, 0x6e, 0x6d, 0x8e, 0xc1, 0x0a,
      0xc2, 0x30, 0x10, 0x44, 0xef, 0x85, 0x7e, 0x8e, 0xc5, 0xeb, 0x8b, 0x4d, 0xd3, 0x80, 0x41, 0xa2,
      0xb6, 0x50, 0x6f, 0x55, 0x31, 0x82, 0x82, 0x1e, 0x84, 0xb4, 0x7f, 0x6f, 0xd3, 0x88, 0xad, 0xe0,
      0x69, 0x99, 0xd9, 0xd9, 0xb7, 0x63, 0x1c, 0x3d, 0x2b, 0x38, 0x79, 0x71, 0xa7, 0xd4, 0xb4, 0x56,
      0x5c, 0x51, 0x3e, 0xcc, 0x0c, 0x25, 0x39, 0x23, 0x9e, 0xa8, 0x25, 0x47, 0x47, 0x46, 0xfe, 0xa0,
      0xb1, 0xa2, 0x47, 0x19, 0xda, 0xe0, 0x97, 0x0d, 0x07, 0x8b, 0x23, 0xb7, 0x68, 0xf0, 0x69, 0x12,
      0x48, 0x86, 0xe0, 0xe8, 0xd1, 0x99, 0x34, 0x41, 0x77, 0x41, 0xaf, 0x2d, 0x8b, 0xcf, 0xc5, 0x8b,
      0x5c, 0x7e, 0x73, 0xbf, 0x79, 0x49, 0x9a, 0x0c, 0x4e, 0xff, 0x8f, 0x30, 0xd3, 0xf1, 0x62, 0xec,
      0x50, 0x4d, 0x7b, 0x1b, 0x09, 0xf1, 0xc3, 0x8d, 0xcd, 0x90, 0x1f, 0xba, 0x45, 0xa7, 0x8b, 0xdd,
      0xc4, 0x0e, 0x59, 0x51, 0x23, 0x6a, 0x0a, 0xcd, 0xde, 0x89, 0x0b, 0x45, 0x83, 0xb5, 0xc2, 0x50,
      0x54, 0x6c, 0x27, 0x82, 0x9f, 0x13, 0xd3, 0xe4, 0x0d, 0x50, 0x4b, 0x01, 0x02, 0x2e, 0x00, 0x14,
      0x00, 0x00, 0x00, 0x08, 0x00, 0xcc, 0x84, 0x32, 0x59, 0x4d, 0xab, 0x01, 0xdc, 0x9d, 0x00, 0x00,
      0x00, 0x30, 0x01, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20,
      0x00, 0xb4, 0x81, 0x00, 0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65,
      0x64, 0x2e, 0x62, 0x69, 0x6e, 0x50, 0x4b, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
      0x00, 0x3c, 0x00, 0x00, 0x00, 0xc9, 0x00, 0x00, 0x00, 0x00, 0x00
    ]

    let sqlQuery9 = "[dbo].[sis_GetDBAccess]"

    let sqlQuery9Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery9],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[8],  # Use ninth query result object
      paramsIn9            # Input parameters in zipped boost format
    )

    if sqlQuery9Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery9, " (status code: ", sqlQuery9Res.status, ")"
      return

    echo sqlQuery9Res.results

    let sqlQuery10 = "[dbo].[sis_ur_GetActiveRole]"

    let sqlQuery10Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery10],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[9],  # Use tenth query result object
      paramsIn9            # Input parameters in zipped boost format
    )

    if sqlQuery10Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery10, " (status code: ", sqlQuery10Res.status, ")"
      return

    echo sqlQuery10Res.results

    let sqlQuery11 = "[dbo].[sis_GetUserID]"

    let sqlQuery11Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery11],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[10],  # Use eleventh query result object
      paramsIn9            # Input parameters in zipped boost format
    )

    if sqlQuery11Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery11, " (status code: ", sqlQuery11Res.status, ")"
      return

    echo sqlQuery11Res.results


    # TODO: Implement DB communication

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
