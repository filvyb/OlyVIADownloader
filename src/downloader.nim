import asyncdispatch
import strutils, os, tables

import comm/[initial, db]
import utils/[boost, zip]

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
    let boostText1 = """
      22 serialization::archive 4 0 0
      1 0 0 0 1 -94 -1 0 0 0 1 2 0 0 1 0 0 0 1 5 1 1"""
    let paramsIn1 = boostBinToZip(boostText1)

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

    let sqlQuery9 = "[dbo].[sis_GetDBAccess]"
    let boostText9 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 1 12 RETURN_VALUE 1 0 1 1"
    let paramsIn9 = boostBinToZip(boostText9)

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
    let sname = if sqlQuery7Res.results.hasKey("SNAME"): sqlQuery7Res.results["SNAME"][0].strip() else: raise newException(ValueError, "SNAME not found in results")
    let boostText11 = "22 serialization::archive 4 0 0 3 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 1 12 RETURN_VALUE 1 0 1 1 1 -98 0 10 szUserName 1 5 1 0 1 8192 1 0 0 1 " & $sname.len & " " & sname
    let paramsIn11 = boostBinToZip(boostText11)

    let sqlQuery11Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery11],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[10],  # Use eleventh query result object
      paramsIn11            # Input parameters in zipped boost format
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
