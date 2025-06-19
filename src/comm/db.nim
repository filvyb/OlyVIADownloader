import faststreams/inputs
import asyncdispatch
import strutils, sequtils, tables

import ../[utils, structs]

import DotNimRemoting/tcp/[client, common]
import DotNimRemoting/msnrbf/[helpers, grammar, enums, types, context]
import DotNimRemoting/msnrbf/records/[methodinv, member, class, arrays]

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
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Check if we have a return value
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      let statusCode = ret.returnValue.value.int32Val
      if statusCode != 0:
        echo "CreateQueryResultObjects failed with status code: ", statusCode
        return @[]
    
    # The response structure includes:
    # 1. An args array (ArraySingleObject) 
    # 2. The Queue object (SystemClassWithMembersAndTypes)
    # 3. The Queue's internal array (ArraySingleObject) containing the IDs
    # Find the Queue object in the referenced records
    var queueObj: RemotingValue
    var queueFound = false

    let merged = responseMsg.methodCallArray & responseMsg.referencedRecords

    for record in merged:
      if record.kind == rvClass:
        let classRecord = record.classVal.record
        if classRecord.kind == rtSystemClassWithMembersAndTypes:
          if classRecord.systemClassWithMembersAndTypes.classInfo.name.value == "System.Collections.Queue":
            queueObj = record
            queueFound = true
            break

    if queueFound:
      # Extract the queue members
      let queueMembers = queueObj.classVal.members
      if queueMembers.len >= 4:  # Ensure we have all members
        let sizeVal = queueMembers[3]  # _size member
        
        if sizeVal.kind == rvPrimitive and sizeVal.primitiveVal.kind == ptInt32:
          let actualCount = sizeVal.primitiveVal.int32Val
          echo "Server created ", actualCount, " query result objects"
          
          # Find the array object that contains the actual IDs
          var resultIds: seq[int32] = @[]
          
          for record in responseMsg.referencedRecords:
            if record.kind == rvArray:
              let arrayRecord = record.arrayVal.record
              if arrayRecord.kind == rtArraySingleObject:
                # This should be the Queue's internal array
                let elements = record.arrayVal.elements
                
                # Extract the IDs (up to actualCount)
                for i in 0..<min(actualCount, elements.len.int32):
                  let elem = elements[i]
                  if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptInt32:
                    resultIds.add(elem.primitiveVal.int32Val)
                
                if resultIds.len > 0:
                  echo "Successfully retrieved ", resultIds.len, " query result object IDs"
                  return resultIds
  
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

  # Create serialization context
  let ctx = newSerializationContext()
  
  # Create the SQL commands array (will get ID 2)
  let sqlArrayElements = sqlCommandTexts.mapIt(RemotingValue(kind: rvString, 
                                                             stringVal: LengthPrefixedString(value: it)))
  
  let sqlArrayValue = RemotingValue(
    kind: rvArray,
    arrayVal: ArrayValue(
      record: ArrayRecord(
        kind: rtArraySingleString,
        arraySingleString: ArraySingleString(
          recordType: rtArraySingleString,
          arrayInfo: ArrayInfo(
            objectId: 0,  # Will be assigned during serialization
            length: sqlCommandTexts.len.int32
          )
        )
      ),
      elements: sqlArrayElements
    )
  )
  
  # Create the ParamsIn data if provided
  var referencedRecords: seq[RemotingValue] = @[sqlArrayValue]
  var paramsInRef: RemotingValue
  var paramsInRefId: int32 = 0
  
  if paramsIn.len > 0:
    # Create an ArraySinglePrimitive of bytes to hold the binary data
    # Convert bytes to primitive values
    let byteElements = paramsIn.mapIt(RemotingValue(
      kind: rvPrimitive, 
      primitiveVal: byteValue(it)
    ))
    
    let paramsInValue = RemotingValue(
      kind: rvArray,
      arrayVal: ArrayValue(
        record: ArrayRecord(
          kind: rtArraySinglePrimitive,
          arraySinglePrimitive: ArraySinglePrimitive(
            recordType: rtArraySinglePrimitive,
            arrayInfo: ArrayInfo(
              objectId: 0,  # Will be assigned during serialization
              length: paramsIn.len.int32
            ),
            primitiveType: ptByte
          )
        ),
        elements: byteElements
      )
    )
    
    referencedRecords.add(paramsInValue)
    # The params array will get ID 4 (after SQL array gets 2 and its string gets 3)
    paramsInRefId = 4
    paramsInRef = RemotingValue(kind: rvReference, idRef: paramsInRefId)
  else:
    paramsInRef = RemotingValue(kind: rvNull)
  
  # Create method call with ArgsIsArray flag
  var flags: MessageFlags = {MessageFlag.ArgsIsArray, MessageFlag.NoContext}
  
  let call = BinaryMethodCall(
    recordType: rtMethodCall,
    messageEnum: flags,
    methodName: newStringValueWithCode("Execute"),
    typeName: newStringValueWithCode("dbapi.dni.ILLRemoteServer, LLDbRemoting, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null")
  )
  
  # Create the arguments array
  let argsArray = @[
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(sessionId)),      # Client
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(connectionId)),   # Connection
    RemotingValue(kind: rvReference, idRef: 2),                                # SqlCommandTexts reference
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(sqlCommandType)), # SqlCommandType
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(sqlCommandSubType)), # SqlCommandSubType
    RemotingValue(kind: rvPrimitive, primitiveVal: int32Value(queryResultId)),  # QueryResult
    paramsInRef,                                                                # ParamsIn (ref)
    RemotingValue(kind: rvNull),                                               # ParamsOut (ref, null in request)
    RemotingValue(kind: rvNull)                                                # Results (ref, null in request)
  ]
  
  # Create the message
  let msg = newRemotingMessage(ctx, 
    methodCall = some(call), 
    callArray = argsArray,
    refs = referencedRecords
  )
  
  # Serialize and send
  let requestData = serializeRemotingMessage(msg, ctx)
  
  echo "Executing SQL command(s)..."
  if sqlCommandTexts.len > 0:
    echo "  First command: ", sqlCommandTexts[0][0..min(50, sqlCommandTexts[0].len-1)], 
         if sqlCommandTexts[0].len > 50: "..." else: ""
  
  let responseData = await client.invoke("Execute", 
                                       "dbapi.dni.ILLRemoteServer, LLDbRemoting, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null", 
                                       false, requestData)
  
  # Parse response
  var input = memoryInput(responseData)
  let responseMsg = readRemotingMessage(input)
  
  if responseMsg.methodReturn.isSome:
    let ret = responseMsg.methodReturn.get
    
    # Extract the return status
    var status: int32 = -1
    if MessageFlag.ReturnValueInline in ret.messageEnum and 
       ret.returnValue.primitiveType == ptInt32:
      status = ret.returnValue.value.int32Val
      if status != 0:
        echo "Execute failed with status code: ", status
        return (status, @[], initTable[string, seq[string]]())
    
    # The response structure based on packet analysis:
    # 1. BinaryMethodReturn with status code (0 = success)
    # 2. ArraySingleObject with 9 items (matching Execute's 9 parameters):
    #    - Items 0-5: null values (Client, Connection, SqlCommandTexts, SqlCommandType, SqlCommandSubType, QueryResult)
    #    - Item 6: MemberReference to ParamsIn (echoed back from request)
    #    - Item 7: MemberReference to ParamsOut (output parameters)
    #    - Item 8: MemberReference to Results (query results)
    # 3. Three ArraySinglePrimitive objects containing ZIP files:
    #    - ParamsIn: echoed back from request
    #    - ParamsOut: output parameters as ZIP containing "serialized.bin"
    #    - Results: query results as ZIP containing "Resultset.bin"
    
    # Find the array references in the method call array
    var paramsInRef: int32 = 0
    var paramsOutRef: int32 = 0
    var resultsRef: int32 = 0
    
    if responseMsg.methodCallArray.len >= 9:
      # Extract references from positions 6, 7, 8 (0-indexed)
      if responseMsg.methodCallArray[6].kind == rvReference:
        paramsInRef = responseMsg.methodCallArray[6].idRef
      if responseMsg.methodCallArray[7].kind == rvReference:
        paramsOutRef = responseMsg.methodCallArray[7].idRef
      if responseMsg.methodCallArray[8].kind == rvReference:
        resultsRef = responseMsg.methodCallArray[8].idRef
    else:
      echo "Unexpected response format: methodCallArray does not contain enough items"
      raise newException(IOError, "Invalid response format from Execute")
    
    # Now find the actual byte arrays in the referenced records
    var paramsOutBytes: seq[byte] = @[]
    var resultsBytes: seq[byte] = @[]
    
    for record in responseMsg.referencedRecords:
      if record.kind == rvArray:
        let arrayRecord = record.arrayVal.record
        if arrayRecord.kind == rtArraySinglePrimitive:
          let arrayPrim = arrayRecord.arraySinglePrimitive
          
          # Check if this is one of our arrays by matching the expected IDs
          # Note: The objectId is set during deserialization
          if arrayPrim.arrayInfo.objectId == paramsOutRef:
            # Extract ParamsOut bytes
            for elem in record.arrayVal.elements:
              if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
                paramsOutBytes.add(elem.primitiveVal.byteVal)
            echo "Extracted ParamsOut: ", paramsOutBytes.len, " bytes"
            
          elif arrayPrim.arrayInfo.objectId == resultsRef:
            # Extract Results bytes
            for elem in record.arrayVal.elements:
              if elem.kind == rvPrimitive and elem.primitiveVal.kind == ptByte:
                resultsBytes.add(elem.primitiveVal.byteVal)
            echo "Extracted Results: ", resultsBytes.len, " bytes"
    
    echo "Execute completed successfully"
    return (status, paramsOutBytes, parseBoostSqlXmlFromZip(resultsBytes).toTable())
  
  raise newException(IOError, "Failed to execute SQL command")
