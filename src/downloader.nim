import asyncdispatch
import strutils, os, tables

import comm/[initial, db, files]
import utils/[boost, zip]

import DotNimRemoting/tcp/client

proc downloader*(address: string, port: int, username, password, database, directory, filter: string) {.async.} =
  echo "Downloading file"
  echo "Address: ", address
  echo "Port: ", port
  echo "Username: ", username
  echo "Password: ", password
  echo "Database: ", database
  echo "Directory: ", directory
  
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

    #echo sqlQuery1Res.results

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

    #echo sqlQuery2Res.results

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

    #echo sqlQuery3Res.results

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

    #echo sqlQuery4Res.results

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

    #echo sqlQuery5Res.results

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
    #echo sqlQuery6Res.results

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

    #echo sqlQuery8Res.results

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

    #echo sqlQuery9Res.results

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

    #echo sqlQuery10Res.results

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

    #echo sqlQuery11Res.results
    let userId = digUpBoostBin(sqlQuery11Res.paramsOut)

    let sqlQuery12 = "[dbo].[sis_OpenDatabaseChecks]"
    let boostText12 = "22 serialization::archive 4 0 0 1 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1"
    let paramsIn12 = boostBinToZip(boostText12)
    let sqlQuery12Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery12],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[11],  # Use twelfth query result object
      paramsIn12            # Input parameters in zipped boost format
    )

    if sqlQuery12Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery12, " (status code: ", sqlQuery12Res.status, ")"
      return

    #echo sqlQuery12Res.results

    let sqlQuery13 = "[dbo].[sis_ReadDBParameter]"
    let boostText13 = "22 serialization::archive 4 0 0 4 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 0 11 szParameter 1 5 1 0 1 8192 1 0 0 1 14 DbSetupEdition 1 -98 1 7 szValue 1 0 1 1 1 -98 1 9 ret_Value 1 0 1"
    let paramsIn13 = boostBinToZip(boostText13)
    let sqlQuery13Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery13],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[12],  # Use thirteenth query result object
      paramsIn13            # Input parameters in zipped boost format
    )

    if sqlQuery13Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery13, " (status code: ", sqlQuery13Res.status, ")"
      return

    #echo sqlQuery13Res.results

    let sqlQuery14 = "SELECT CONVERT(INT, SERVERPROPERTY ('IsFullTextInstalled')) AS FULL_TEXT_PROP"
    let sqlQuery14Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery14],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[13],  # Use fourteenth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery14Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery14, " (status code: ", sqlQuery14Res.status, ")"
      return

    #echo sqlQuery14Res.results

    # maybe
    #maxBufferedRows = await getMaxBufferedRowCount(client, sessionId, queryResultIds[13])

    # 561
    # get user id twice here?

    let sqlQuery15 = """SELECT vw_DocumentIOHandlers.[ModuleName], 
                                vw_DocumentIOHandlers.[DocIoType], 
                                vw_DocumentIOHandlers.[Version], 
                                vw_DocumentIOHandlers.[Position] 
                                FROM [dbo].[vw_DocumentIOHandlers] 
                                ORDER BY [Position] asc"""
    let sqlQuery15Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery15],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[14],  # Use fifteenth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery15Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery15, " (status code: ", sqlQuery15Res.status, ")"
      return

    #echo sqlQuery15Res.results

    let sqlQuery16 = """SELECT vw_AttributeHandlers.[ModuleName], vw_AttributeHandlers.[Version], vw_AttributeHandlers.[Position] 
                                FROM [dbo].[vw_AttributeHandlers] 
                                ORDER BY [Position] asc"""
    let sqlQuery16Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery16],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[15],  # Use sixteenth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery16Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery16, " (status code: ", sqlQuery16Res.status, ")"
      return

    #echo sqlQuery16Res.results

    let sqlQuery17 = "SELECT vw_Languages.[id_Language], vw_Languages.[name_Language], vw_Languages.[is_DefaultLanguage] FROM [dbo].[vw_Languages]"

    let sqlQuery17Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery17],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[16],  # Use seventeenth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery17Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery17, " (status code: ", sqlQuery17Res.status, ")"
      return

    #echo sqlQuery17Res.results # there's only one lol

    let sqlQuery18 = "set language us_english"
    let sqlQuery18Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery18],        # SQL commands array
      3,                  # SqlCommandType
      50,                 # SqlCommandSubType  
      queryResultIds[17],  # Use eighteenth query result object
      paramsIn1            # Input parameters in zipped boost format
    )

    if sqlQuery18Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery18, " (status code: ", sqlQuery18Res.status, ")"
      return

    #echo sqlQuery18Res.results
    # 601
    let sqlQuery19 = "[dbo].[sis_ReadDbObjects]"
    let langid = if sqlQuery17Res.results.hasKey("id_Language"): sqlQuery17Res.results["id_Language"][0].strip() else: raise newException(ValueError, "id_Language not found in results")
    let boostText19 = "22 serialization::archive 4 0 0 3 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 0 19 id_objecttype_param 1 2 1 0 1 5 1 7 1 -98 0 17 id_language_param 1 2 1 0 1 5 1 " & langid
    let paramsIn19 = boostBinToZip(boostText19)
    let sqlQuery19Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery19],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[18],  # Use nineteenth query result object
      paramsIn19            # Input parameters in zipped boost format
    )

    if sqlQuery19Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery19, " (status code: ", sqlQuery19Res.status, ")"
      return

    #echo sqlQuery19Res.results

    # ReadDbObjects maybe thrice, not sure why
    let sqlQuery20 = "SELECT vw_Picklist.[RKey], vw_Picklist.[Datatype], vw_Picklist.[Flags], vw_Picklist.[GUID] FROM [dbo].[vw_Picklist]"
    let sqlQuery20Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery20],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[19],  # Use twentieth query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery20Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery20, " (status code: ", sqlQuery20Res.status, ")"
      return
    #echo sqlQuery20Res.results

    # Loop through all RKey values from sqlQuery20
    let sqlQuery21 = "SELECT vw_PicklistValues.[Value] FROM [dbo].[vw_PicklistValues] WHERE [RKey] = :B0 ORDER BY [Position] asc"
    
    if not sqlQuery20Res.results.hasKey("RKey"):
      raise newException(ValueError, "RKey not found in results")
    
    let rkeys = sqlQuery20Res.results["RKey"]
    for i, rkey21 in rkeys:
      let rkeyStripped = rkey21.strip()
      echo "Processing RKey [", i + 1, "/", rkeys.len, "]: ", rkeyStripped
      
      let boostText21 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 5 1 0 1 8192 1 0 0 1 " & $rkeyStripped.len & " " & rkeyStripped
      let paramsIn21 = boostBinToZip(boostText21)
      
      let queryResultIndex = 20 + (i mod 10)  # Cycle through query result objects 20-29
      let sqlQuery21Res = await executeSql(
        client, 
        sessionId, 
        connectionId,
        @[sqlQuery21],        # SQL commands array
        1,                  # SqlCommandType
        1,                 # SqlCommandSubType  
        queryResultIds[queryResultIndex],  # Use different query result objects
        paramsIn21            # Input parameters in zipped boost format
      )
      if sqlQuery21Res.status != 0:
        echo "Failed to execute SQL query for RKey ", rkeyStripped, ": ", sqlQuery21, " (status code: ", sqlQuery21Res.status, ")"
        continue
      echo "Results for RKey ", rkeyStripped, ": "
      #echo sqlQuery21Res.results
    
    let sqlQuery22 = """SELECT vw_RecordTypeAndAttrTbl.[AttributeTableRKey], 
                             vw_RecordTypeAndAttrTbl.[RecordTypeRKey], 
                             vw_RecordTypeAndAttrTbl.[RelationType] 
                             FROM [dbo].[vw_RecordTypeAndAttrTbl]"""
    let sqlQuery22Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery22],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[29],  # Use thirtieth query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery22Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery22, " (status code: ", sqlQuery22Res.status, ")"
      return
    #echo sqlQuery22Res.results

    #[ let boostText23 = "22 serialization::archive 4 0 0 3 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 0 19 id_objecttype_param 1 2 1 0 1 5 1 1 1 -98 0 17 id_language_param 1 2 1 0 1 5 1 " & langid
    let paramsIn23 = boostBinToZip(boostText23)
    let sqlQuery23Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery19],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[30],  # Use thirty-first query result object
      paramsIn23            # Input parameters in zipped boost format
    )
    if sqlQuery23Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery19, " (status code: ", sqlQuery23Res.status, ")"
      return
    echo toSeq(keys(sqlQuery23Res.results)) ]#

    #[ let boostText24 = "22 serialization::archive 4 0 0 3 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 0 19 id_objecttype_param 1 2 1 0 1 5 1 2 1 -98 0 17 id_language_param 1 2 1 0 1 5 1 " & langid
    let paramsIn24 = boostBinToZip(boostText24)
    let sqlQuery24Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery19],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[31],  # Use thirty-second query result object
      paramsIn24            # Input parameters in zipped boost format
    )
    if sqlQuery24Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery19, " (status code: ", sqlQuery24Res.status, ")"
      return
    echo toSeq(keys(sqlQuery24Res.results)) ]#

    let sqlQuery25 = "SELECT vw_Constraints.[RecTypeMain], vw_Constraints.[RecTypeChild], vw_Constraints.[AcceptAsChild], vw_Constraints.[VisibleAsChild], vw_Constraints.[SearchableAsChild] FROM [dbo].[vw_Constraints] ORDER BY [RecTypeMain] asc"
    let sqlQuery25Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery25],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[32],  # Use thirty-third query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery25Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery25, " (status code: ", sqlQuery25Res.status, ")"
      return
    #echo sqlQuery25Res.results

    let sqlQuery26 = """SELECT vw_RecordTypeAndAttrTbl.[AttributeTableRKey], 
                                       vw_RecordTypeAndAttrTbl.[RecordTypeRKey], 
                                       vw_RecordTypeAndAttrTbl.[RelationType] 
                                FROM [dbo].[vw_RecordTypeAndAttrTbl]"""
    let sqlQuery26Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery26],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[33],  # Use thirty-fourth query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery26Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery26, " (status code: ", sqlQuery26Res.status, ")"
      return
    #echo sqlQuery26Res.results
    
    # 1357
    let sqlQuery27 = "[dbo].[sis_ur_GetUserActiveRole]"
    let sqlQuery27Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery27],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[34],  # Use thirty-fifth query result object
      paramsIn11            # Input parameters in zipped boost format
    )
    if sqlQuery27Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery27, " (status code: ", sqlQuery27Res.status, ")"
      return
    #echo sqlQuery27Res.results

    let sqlQuery28 = "[dbo].[sis_ReadSettings]"
    let boostText28 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -98 0 7 nUserID 1 2 1 0 1 5 1 2" # 2 is the user ID, but I don't know how to get it from a previous query
    let paramsIn28 = boostBinToZip(boostText28)
    let sqlQuery28Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery28],        # SQL commands array
      2,                  # SqlCommandType
      71,                 # SqlCommandSubType  
      queryResultIds[35],  # Use thirty-sixth query result object
      paramsIn28            # Input parameters in zipped boost format
    )
    if sqlQuery28Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery28, " (status code: ", sqlQuery28Res.status, ")"
      return
    #echo sqlQuery28Res.results

    # second read settings with different user ID?

    let sqlQuery29 = "[dbo].[sis_SetStringProcParam]"

    let sqlQuery30 = "[dbo].[sis_SetIntProcParam]" # probably used to set sql that's executed with BeginTran and CommitTran

    let sqlQuery31 = "[dbo].[sis_ReadAllUserForms]"

    let sqlQuery32 = "[dbo].[sis_GetIntProcParam]"

    let sqlQuery33 = "[dbo].[sis_GetStringProcParam]"

    let sqlQuery34 = "[dbo].[sis_DescribeProc]"

    let sqlQuery35 = "[dbo].[sis_Exlv_CleanupTimeouts]"
    # 1532
    let sqlQuery36 = "[dbo].[sis_Exlv_DeleteLogEntry]"

    let sqlQuery37 = "[dbo].[sis_ur_GetUserActiveRole]"

    let sqlQuery38 = "[dbo].[sis_Exlv_CleanupTimeouts]"

    let sqlQuery39 = "[dbo].[sis_Exlv_GetStateInfo]"
    # 1625
    let sqlQuery40 = "[dbo].[sis_Exlv_WriteLogEntryEx]"

    # begin transaction
    # commit transaction
    # maybe not needed

    let sqlQuery41 = "[dbo].[sis_GetAvailRecordGroups]"

    let sqlQuery42 = "SELECT vw_Roles.[name_Role] FROM [dbo].[vw_Roles]"
    let sqlQuery42Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery42],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[36],  # Use thirty-seventh query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery42Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery42, " (status code: ", sqlQuery42Res.status, ")"
      return
    #echo sqlQuery42Res.results

    let sqlQuery43 = """SELECT vw_UsersInRole.[name_User], 
                                       vw_UsersInRole.[desc_User], 
                                       vw_UsersInRole.[is_Group] 
                                FROM [dbo].[vw_UsersInRole] 
                                WHERE vw_UsersInRole.[name_Role] = :B0"""
    
    if not sqlQuery42Res.results.hasKey("name_Role"):
      raise newException(ValueError, "name_Role not found in results")
    let roles = sqlQuery42Res.results["name_Role"]
    for i, role in roles:
      let roleStripped = role.strip()
      echo "Processing role [", i + 1, "/", roles.len, "]: ", roleStripped
      let boostText43 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 5 1 0 1 8192 1 0 0 1 " & $roleStripped.len & " " & roleStripped
      let paramsIn43 = boostBinToZip(boostText43)
      let queryResultIndex = 37 + (i mod 10)  # Cycle through query result objects 37-46
      let sqlQuery43Res = await executeSql(
        client, 
        sessionId, 
        connectionId,
        @[sqlQuery43],        # SQL commands array
        1,                  # SqlCommandType
        1,                 # SqlCommandSubType  
        queryResultIds[queryResultIndex],  # Use different query result objects
        paramsIn43            # Input parameters in zipped boost format
      )
      if sqlQuery43Res.status != 0:
        echo "Failed to execute SQL query for role ", roleStripped, ": ", sqlQuery43, " (status code: ", sqlQuery43Res.status, ")"
        continue
      echo "Results for role ", roleStripped, ": "
      echo sqlQuery43Res.results
    # 1724
    let sqlQuery44 = """SELECT vw_NetImgServers.[id_Server], vw_NetImgServers.[ServerName], 
                               vw_NetImgServers.[Protocol], vw_NetImgServers.[Port], 
                               vw_NetImgServers.[DbGUID], vw_NetImgServers.[ProtocolVersion], 
                               vw_NetImgServers.[BuildNo] 
                        FROM [dbo].[vw_NetImgServers] 
                        WHERE [id_Server] = :B0"""
    # this contains the server ID, but I don't know where it is and how to get it from a previous query, but here it's 1
    let boostText44 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 2 1 0 1 5 1 1"
    let paramsIn44 = boostBinToZip(boostText44)
    let sqlQuery44Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery44],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[47],  # Use thirty-eighth query result object
      paramsIn44            # Input parameters in zipped boost format
    )
    if sqlQuery44Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery44, " (status code: ", sqlQuery44Res.status, ")"
      return
    #echo sqlQuery44Res.results

    #[ let sqlQuery45 = "SELECT t1.[attRecID], t1.[attRecName] FROM [dbo].[vw_AttributeTable_1] AS [t1] WHERE ((t1.[attRecRecordTypeID] =  :B1)OR (t1.[attRecRecordTypeID] =  :B0))"
    let boostText45 = "22 serialization::archive 4 0 0 3 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 2 1 0 1 5 1 13 1 -99 -1 2 B1 1 2 1 0 1 5 1 6"
    let paramsIn45 = boostBinToZip(boostText45)
    let sqlQuery45Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery45],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[48],  # Use thirty-ninth query result object
      paramsIn45            # Input parameters in zipped boost format
    )
    if sqlQuery45Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery45, " (status code: ", sqlQuery45Res.status, ")"
      return
    echo sqlQuery45Res.results ]#

    #2349
    #let sqlQuery46 = "SELECT t1.[attRecID] FROM [dbo].[vw_AttributeTable_1] AS [t1] WHERE [attRecParentID] = :B0 ORDER BY [t1].[attRecID] asc"
    #let sqlQuery46 = "SELECT t1.[attRecID], t1.[attRecName] FROM [dbo].[vw_AttributeTable_1] AS [t1] WHERE [attRecRecordType] = 'rctpImg' AND (t1.[attPreviousRecordID] IS NOT NULL OR t1.[attPreviousRecordID] != '')"
    #let sqlQuery46 = "SELECT t1.[attRecGUID], t1.[attRecID], t1.[attRecName], t1.[attRecParentID] FROM [dbo].[vw_AttributeTable_1] AS [t1] WHERE t1.[attRecRecordType] = 'rctpFolder' OR t1.[attRecRecordType] = 'rctpImg' ORDER BY t1.[attRecID] asc"
    let sqlQuery46 = "SELECT t1.[attRecGUID], t1.[attRecID] FROM [dbo].[vw_AttributeTable_1] AS [t1] WHERE t1.[attRecRecordType] = 'rctpImg' ORDER BY t1.[attRecID] asc"
    let boostText46 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 3 1 0 1 7 1 389"
    let paramsIn46 = boostBinToZip(boostText46)
    let sqlQuery46Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery46],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[49],  # Use fortieth query result object
      paramsIn1            # Input parameters in zipped boost format
    )
    if sqlQuery46Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery46, " (status code: ", sqlQuery46Res.status, ")"
      return
    #echo sqlQuery46Res.results
    #tableToCsv(sqlQuery46Res.results, "attribute_table_1.csv")
    
    var imageEntries = initTable[string, seq[string]]()
    if not sqlQuery46Res.results.hasKey("attRecID"):
      raise newException(ValueError, "attRecID not found in results")
    for attRecID in sqlQuery46Res.results["attRecID"]:
      #3901
      let sqlQuery47 = """SELECT tb_DocumentIOType_5.[attRecID], 
                                        tb_DocumentIOType_5.[id_NetImgServer], 
                                        tb_DocumentIOType_5.[VolumeGUID], 
                                        tb_DocumentIOType_5.[GUID], 
                                        tb_DocumentIOType_5.[FileName], 
                                        tb_DocumentIOType_5.[EntryType] 
                                  FROM [dbo].[tb_DocumentIOType_5] 
                                  WHERE [attRecID] = :B0"""
      #let attRecID = 5384
      let boostText47 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 3 1 0 1 7 1 " & $attRecID.strip()
      let paramsIn47 = boostBinToZip(boostText47)
      let sqlQuery47Res = await executeSql(
        client, 
        sessionId, 
        connectionId,
        @[sqlQuery47],        # SQL commands array
        1,                  # SqlCommandType
        1,                 # SqlCommandSubType  
        queryResultIds[50],  # Use forty-first query result object
        paramsIn47            # Input parameters in zipped boost format
      )
      if sqlQuery47Res.status != 0:
        echo "Failed to execute SQL query: ", sqlQuery47, " (status code: ", sqlQuery47Res.status, ")"
        return
      #echo sqlQuery47Res.results
      for key, value in sqlQuery47Res.results: # not as performant but avoids nested loops
        if not imageEntries.hasKey(key):
          imageEntries[key] = @[]
        imageEntries[key].add(value)

    #echo "Image entries:\n", imageEntries
    #raise newException(ValueError, "Image entries processing not implemented yet")
    # 3934
    #[ let sqlQuery47 = """SELECT tb_SubDocuments_IOType_5.[ID], 
                                       tb_SubDocuments_IOType_5.[attRecID], 
                                       tb_SubDocuments_IOType_5.[id_NetImgServer], 
                                       tb_SubDocuments_IOType_5.[VolumeGUID], 
                                       tb_SubDocuments_IOType_5.[GUID], 
                                       tb_SubDocuments_IOType_5.[RelativePath], 
                                       tb_SubDocuments_IOType_5.[FileName], 
                                       tb_SubDocuments_IOType_5.[FileSize], 
                                       tb_SubDocuments_IOType_5.[EntryType] 
                                FROM [dbo].[tb_SubDocuments_IOType_5] 
                                WHERE [attRecID] = :B0"""
    let boostText47 = "22 serialization::archive 4 0 0 2 0 0 0 1 -94 -1 0  0 0 1 2 0 0 1 0 0 0 1 5 1 1 1 -99 -1 2 B0 1 3 1 0 1 7 1 5384"
    let paramsIn47 = boostBinToZip(boostText47)
    let sqlQuery47Res = await executeSql(
      client, 
      sessionId, 
      connectionId,
      @[sqlQuery47],        # SQL commands array
      1,                  # SqlCommandType
      1,                 # SqlCommandSubType  
      queryResultIds[49],  # Use fortieth query result object
      paramsIn47            # Input parameters in zipped boost format
    )
    if sqlQuery47Res.status != 0:
      echo "Failed to execute SQL query: ", sqlQuery47, " (status code: ", sqlQuery47Res.status, ")"
      return
    echo sqlQuery47Res.results ]#

    let serverUrl = if sqlQuery44Res.results.hasKey("DbGUID"): sqlQuery44Res.results["DbGUID"][0].strip() else: raise newException(ValueError, "DbGUID not found in results")
    let imageGuids = if imageEntries.hasKey("GUID"): imageEntries["GUID"] else: raise newException(ValueError, "GUID not found in image entries")
    for databaseImageGuid in imageGuids:
      let databaseImageGuid = databaseImageGuid.strip()
      echo "Image GUID: ", databaseImageGuid
      #3954
      let isImageValid = await isImageValid(client, databaseImageGuid, serverUrl)
      if not isImageValid:
        echo "Invalid image GUID: ", databaseImageGuid
      #3969
      let fileListResult = await getFileList(client, serverUrl, databaseImageGuid)
      for file in fileListResult.fileNames:
        if not file.contains(filter):
          echo "Skipping file due to filter: ", file
          continue
        echo "File: ", file
        #continue # skip downloading files for now
        var filePath = directory / databaseImageGuid
        if file.contains("\\"):
          var parts = file.split("\\")
          for part in parts:
            filePath = filePath / part
        else:
          filePath = filePath / file
        let fileStat = await downloadFilePipelined(client, serverUrl, databaseImageGuid, file, filePath)
        if fileStat:
          echo "File downloaded successfully: ", file
        else:
          echo "Failed to download file: ", file

  except Exception as e:
    echo "Error: ", e.msg
  finally:
    # Gracefully disconnect
    if sessionId != 0 and connectionId != 0:
      try:
        let disconnectStatus = await disconnect(client, sessionId, connectionId)
        if disconnectStatus != 0:
          echo "Warning: Disconnect returned non-zero status: ", disconnectStatus
          
        let destroyStatus = await destroyConnectionObject(client, sessionId, connectionId)
        if destroyStatus != 0:
          echo "Warning: DestroyConnectionObject returned non-zero status: ", destroyStatus
      except Exception as e:
        echo "Error during cleanup: ", e.msg
    
    await client.close()
    echo "Connection closed"
