import asyncdispatch, options
import strutils, tables

import ../utils/[zip, boost]

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums]
import DotNimRemoting/msnrbf/records/methodinv

proc dissolveAlias*(client: NrtpTcpClient, alias: string): Future[tuple[status: int32, dbName: string, dbLocation: string, dbmsType: int32]] {.async.} =
  ## Resolves a database alias to its actual database connection information
  ## Returns a tuple with the status code, database name, database location, and DBMS type
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create method call with the required parameters
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: alias and placeholders for ref parameters
  let args = @[
    toRemotingValue(alias),  # Input alias parameter
    nullValue(),             # Placeholder for ref database name
    nullValue(),             # Placeholder for ref database location
    toRemotingValue(0'i32)   # Placeholder for ref DBMS type
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
  let msg = deserializeRemotingMessage(responseData)
  
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
  let args = @[
    toRemotingValue(sessionId),      # Client/Session ID
    toRemotingValue(connectionId),   # Connection ID
    if dbName == "": nullValue() else: toRemotingValue(dbName),  # DatabaseName (NULL if empty)
    toRemotingValue(dbLocation),     # DatabaseLocation
    toRemotingValue(dbmsType),       # DBMSType
    toRemotingValue(username),       # User
    toRemotingValue(password),       # Password
    if domain == "": nullValue() else: toRemotingValue(domain)   # Domain (NULL if empty)
  ]

  echo "Connecting to database at: ", dbLocation
  let ret = await client.call("ConnectEx", typeName, args)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let statusCode = ret.getInt32
    if statusCode == 0:
      echo "Successfully connected to database"
    else:
      echo "Failed to connect to database, error code: ", statusCode
    return statusCode

  echo "Failed to connect to database, unexpected response"
  raise newException(IOError, "Failed to connect to database")

proc createQueryResultObjects*(client: NrtpTcpClient, sessionId: int32, count: int32): Future[seq[int32]] {.async.} =
  ## Creates query result objects on the server
  ## Returns a sequence of object IDs that can be used for queries
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  # Build the Queue's backing array (32-slot initial capacity, all nulls)
  var slots: seq[RemotingValue]
  for i in 0..<32:
    slots.add(nullValue())

  # Empty System.Collections.Queue instance the server fills with object IDs
  let queue = systemClassValue("System.Collections.Queue", {
    "_array": objectArrayValue(slots),
    "_head": toRemotingValue(0'i32),
    "_tail": toRemotingValue(0'i32),
    "_size": toRemotingValue(0'i32),
    "_growFactor": toRemotingValue(200'i32),  # 200 = 2.0 as percentage
    "_version": toRemotingValue(0'i32)
  })

  let args = @[
    toRemotingValue(sessionId),  # Client
    toRemotingValue(count),      # nCount
    queue                        # Queue receiving the created IDs
  ]

  let requestData = createMethodCallRequest(
    methodName = "CreateQueryResultObjects",
    typeName = typeName,
    args = args
  )

  echo "Creating ", count, " query result objects..."
  let responseData = await client.invoke("CreateQueryResultObjects", typeName, false, requestData)

  # Parse response
  let responseMsg = deserializeRemotingMessage(responseData)

  # Status code travels as the inline return value; returnValueOf also
  # resolves member references in the response graph
  let retVal = returnValueOf(responseMsg)
  if retVal.kind == rvPrimitive and retVal.primitiveVal.kind == ptInt32:
    let statusCode = retVal.getInt32
    if statusCode != 0:
      echo "CreateQueryResultObjects failed with status code: ", statusCode
      return @[]

  # The created IDs come back inside the returned Queue's _array member;
  # find the Queue among the response records
  for record in responseMsg.methodCallArray & responseMsg.referencedRecords:
    if record.kind == rvClass and className(record) == "System.Collections.Queue":
      let sizeVal = record["_size"]
      if sizeVal.kind != rvPrimitive or sizeVal.primitiveVal.kind != ptInt32:
        break
      let actualCount = sizeVal.getInt32
      echo "Server created ", actualCount, " query result objects"

      let buffer = record["_array"]
      if buffer.kind != rvArray:
        break

      var resultIds: seq[int32] = @[]
      for i in 0..<min(actualCount.int, buffer.len):
        let elem = buffer[i]
        if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptInt32:
          resultIds.add(elem.getInt32)

      if resultIds.len > 0:
        echo "Successfully retrieved ", resultIds.len, " query result object IDs"
        return resultIds
      break

  return @[]

proc executeSql*(client: NrtpTcpClient, sessionId: int32, connectionId: int32,
             sqlCommandTexts: seq[string], sqlCommandType: int32, sqlCommandSubType: int32,
             queryResultId: int32, paramsIn: seq[byte] = @[]): 
             Future[tuple[status: int32, paramsOut: seq[byte], results: Table[string, seq[string]]]] {.async.} =
  ## Executes SQL commands on the server
  ## Parameters:
  ##   sessionId - The session ID (Client parameter)
  ##   connectionId - The connection ID
  ##   sqlCommandTexts - Array of SQL command strings to execute
  ##   sqlCommandType - Type of SQL command
  ##   sqlCommandSubType - Subtype of SQL command
  ##   queryResultId - ID of the query result object to use
  ##   paramsIn - Optional input parameters as binary data (typically a zip file)
  ## Returns a tuple with:
  ##   status - Status code (0 for success)
  ##   paramsOut - Output parameters as binary data (typically a zip file)
  ##   results - Parsed SQL result set containing columns and rows
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  # Create the arguments according to the Execute method signature; the SQL
  # commands travel as a string[] and ParamsIn as a byte[] (null when empty)
  let args = @[
    toRemotingValue(sessionId),          # Client
    toRemotingValue(connectionId),       # Connection
    toRemotingValue(sqlCommandTexts),    # SqlCommandTexts
    toRemotingValue(sqlCommandType),     # SqlCommandType
    toRemotingValue(sqlCommandSubType),  # SqlCommandSubType
    toRemotingValue(queryResultId),      # QueryResult
    if paramsIn.len > 0: toRemotingValue(paramsIn) else: nullValue(),  # ParamsIn
    nullValue(),                         # ParamsOut (ref, null in request)
    nullValue()                          # Results (ref, null in request)
  ]

  let requestData = createMethodCallRequest(
    methodName = "Execute",
    typeName = typeName,
    args = args
  )

  echo "Executing SQL command(s)..."
  if sqlCommandTexts.len > 0:
    echo "  First command: ", sqlCommandTexts[0][0..min(50, sqlCommandTexts[0].len-1)],
         if sqlCommandTexts[0].len > 50: "..." else: ""

  let responseData = await client.invoke("Execute", typeName, false, requestData)

  # Parse response
  let responseMsg = deserializeRemotingMessage(responseData)
  
  if responseMsg.methodReturn.isSome:
    var status: int32 = -1
    let retVal = returnValueOf(responseMsg)
    if retVal.kind == rvPrimitive and retVal.primitiveVal.kind == ptInt32:
      status = retVal.getInt32
      if status != 0:
        echo "Execute failed with status code: ", status
        return (status, @[], initTable[string, seq[string]]())

    # The output args array mirrors Execute's 9 parameters:
    #   - Items 0-5: null values (echoed input parameters)
    #   - Item 6: ParamsIn (echoed back from request)
    #   - Item 7: ParamsOut as ZIP containing "serialized.bin"
    #   - Item 8: Results as ZIP containing "Resultset.bin"
    # References are already resolved, so the byte arrays sit in place
    if responseMsg.methodCallArray.len < 9:
      echo "Unexpected response format: methodCallArray does not contain enough items"
      raise newException(IOError, "Invalid response format from Execute")

    var paramsOutBytes: seq[byte] = @[]
    var resultsBytes: seq[byte] = @[]

    let paramsOutVal = responseMsg.methodCallArray[7]
    if paramsOutVal.kind == rvArray:
      for elem in paramsOutVal.elements:
        if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
          paramsOutBytes.add(elem.getByte)
      echo "Extracted ParamsOut: ", paramsOutBytes.len, " bytes"

    let resultsVal = responseMsg.methodCallArray[8]
    if resultsVal.kind == rvArray:
      for elem in resultsVal.elements:
        if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
          resultsBytes.add(elem.getByte)
      echo "Extracted Results: ", resultsBytes.len, " bytes"

    echo "Execute completed successfully"
    #echo digUpBoostBin(paramsOutBytes)
    #echo digUpBoostBin(resultsBytes)
    #echo $parseBoostSqlXmlFromZip(resultsBytes).columns
    #echo $parseBoostSqlXmlFromZip(resultsBytes).rows
    return (status, paramsOutBytes, parseBoostSqlXmlFromZip(resultsBytes).toTable())
  
  raise newException(IOError, "Failed to execute SQL command")

proc getMaxBufferedRowCount*(client: NrtpTcpClient, sessionId: int32, queryResultId: int32): 
                           Future[tuple[status: int32, maxBufferedRows: uint32]] {.async.} =
  ## Gets the maximum number of buffered rows for a query result object
  ## Parameters:
  ##   sessionId - The session ID (Client parameter)
  ##   queryResultId - The query result handle (QueryHandle parameter)
  ## Returns a tuple with:
  ##   status - Status code (0 for success)
  ##   maxBufferedRows - The maximum number of buffered rows
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create method call with the required parameters
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"
  
  # Create arguments: sessionId, queryResultId, and placeholder for ref parameter
  let args = @[
    toRemotingValue(sessionId),      # Client parameter
    toRemotingValue(queryResultId),  # QueryHandle parameter
    toRemotingValue(0'u32)           # Placeholder for ref uMaxBufferedRows parameter
  ]

  # Create request
  let requestData = createMethodCallRequest(
    methodName = "GetMaxBufferedRowCount",
    typeName = typeName,
    args = args
  )

  echo "Getting max buffered row count for query result ID: ", queryResultId
  let responseData = await client.invoke("GetMaxBufferedRowCount", typeName, false, requestData)

  # Parse response
  let msg = deserializeRemotingMessage(responseData)
  
  if msg.methodReturn.isSome:
    let ret = msg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      
      # Check if we have arguments in the response
      if MessageFlag.ArgsInline in ret.messageEnum and ret.args.len >= 3:
        # Extract the reference parameter
        # Note: The first two parameters are returned as null (unchanged input values)
        let maxBufferedRows = ret.args[2].value.uint32Val  # Third argument is the output value
        
        if statusCode == 0:
          echo "Max buffered rows: ", maxBufferedRows
        else:
          echo "GetMaxBufferedRowCount failed with status code: ", statusCode
          
        return (statusCode, maxBufferedRows)
      
      raise newException(IOError, "Invalid response format: not enough arguments returned")
  
  raise newException(IOError, "Failed to get max buffered row count")

proc beginTran*(client: NrtpTcpClient, clientId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Begins a transaction on the remote server
  ## 
  ## Parameters:
  ##   - clientId: The client identifier
  ##   - connectionId: The connection identifier
  ## 
  ## Returns:
  ##   Transaction ID (typically 1 for success)
  
  # The type name from the capture includes the full assembly info
  # But the request builder appends version info, so we need just the base part
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  let ret = await client.call("BeginTran", typeName, clientId, connectionId)

  if ret.kind == rvPrimitive:
    if ret.primitiveVal.kind == ptInt32:
      return ret.getInt32
    else:
      raise newException(ValueError, "Expected Int32 return value, got: " & $ret.primitiveVal.kind)

  raise newException(IOError, "Invalid response from BeginTran")

proc commitTran*(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Commits a transaction on the database connection
  ## Parameters:
  ##   sessionId - The session ID (Client parameter)
  ##   connectionId - The connection ID
  ## Returns the status code (1 for success based on the captured packet)
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "CommitTran" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  echo "Committing transaction for session ID: ", sessionId, ", connection ID: ", connectionId
  let ret = await client.call("CommitTran", typeName, sessionId, connectionId)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let statusCode = ret.getInt32
    if statusCode == 1:
      echo "Transaction committed successfully"
    else:
      echo "Transaction commit returned status: ", statusCode
    return statusCode

  echo "Failed to commit transaction, unexpected response"
  raise newException(IOError, "Failed to commit transaction")

proc inTran*(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Checks if a transaction is currently active on the database connection
  ## Parameters:
  ##   sessionId - The session ID (Client parameter)
  ##   connectionId - The connection ID
  ## Returns the status code
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "InTran" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  echo "Checking if in transaction for session ID: ", sessionId, ", connection ID: ", connectionId
  let ret = await client.call("InTran", typeName, sessionId, connectionId)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let statusCode = ret.getInt32
    if statusCode == 1:
      echo "Transaction is active"
    else:
      echo "No active transaction"
    return statusCode

  echo "Failed to check transaction status, unexpected response"
  raise newException(IOError, "Failed to check transaction status")

proc rollbackTran*(client: NrtpTcpClient, sessionId: int32, connectionId: int32): Future[int32] {.async.} =
  ## Rolls back a transaction on the database connection
  ## Parameters:
  ##   sessionId - The session ID (Client parameter)
  ##   connectionId - The connection ID
  ## Returns the status code (1 for success based on the captured packet)
  
  # Set the path to the server object
  client.setPath("LLRemoteServer")

  # Create a "RollbackTran" method call
  let typeName = "dbapi.dni.ILLRemoteServer, LLDbRemoting"

  echo "Rolling back transaction for session ID: ", sessionId, ", connection ID: ", connectionId
  let ret = await client.call("RollbackTran", typeName, sessionId, connectionId)

  if ret.kind == rvPrimitive and ret.primitiveVal.kind == ptInt32:
    let statusCode = ret.getInt32
    if statusCode == 1:
      echo "Transaction rolled back successfully"
    else:
      echo "Transaction rollback returned status: ", statusCode
    return statusCode

  echo "Failed to roll back transaction, unexpected response"
  raise newException(IOError, "Failed to roll back transaction")
