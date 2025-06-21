import std/[sequtils, strutils, xmlparser, xmltree, tables, json]
import ../structs

proc parseFlagXml*(xmlContent: string): JsonNode =
  ## Parse PropertySet XML and convert to JSON
  ## Returns a JSON object with properties
  result = newJObject()
  
  try:
    let doc = parseXml(xmlContent)
    let propertySetNode = if doc.tag == "PropertySet": doc else: doc.child("PropertySet")
    
    if propertySetNode != nil:
      var properties = newJArray()
      
      for propNode in propertySetNode.findAll("Property"):
        var property = newJObject()
        echo "Processing Property node: ", propNode
        
        # Get the ID attribute
        let idAttr = propNode.attr("ID")
        if idAttr != "":
          property["ID"] = newJString(idAttr)
        
        # Get PName
        let pNameNode = propNode.child("PName")
        if pNameNode != nil:
          property["PName"] = newJString(pNameNode.innerText)
        
        # Get value (could be dword, string, etc.)
        for child in propNode:
          if child.tag != "PName":
            property["ValueType"] = newJString(child.tag)
            property["Value"] = newJString(child.innerText)
            break
        
        properties.add(property)
      
      result["Properties"] = properties
    
  except CatchableError:
    # If parsing fails, return error info
    result["error"] = newJString("Failed to parse PropertySet XML")

proc extractValue(node: XmlNode): string =
  ## Extract the actual value from the deeply nested structure
  # Navigate through: second -> cvaluecbin -> pValue -> value
  let secondNode = node.child("second")
  if secondNode == nil: return ""
  
  let cvaluecbinNode = secondNode.child("cvaluecbin")
  if cvaluecbinNode == nil: return ""
  
  let pValueNode = cvaluecbinNode.child("pValue")
  if pValueNode == nil: return ""
  
  let valueNode = pValueNode.child("value")
  if valueNode == nil: return ""
  
  # Check if there's a <text> child element first
  let textNode = valueNode.child("text")
  if textNode != nil:
    let textContent = textNode.innerText
    
    # Check if the text content is XML by looking for XML declaration or root elements
    if textContent.contains("<?xml") or textContent.contains("&lt;?xml") or (textContent.contains("<") and textContent.contains(">")):
      # Try to parse as PropertySet XML and convert to JSON
      try:
        let jsonResult = parseFlagXml(textContent)
        return $jsonResult
      except:
        # If parsing fails, return the original text
        return textContent
    else:
      return textContent
  else:
    # Fall back to extracting the entire value content
    return valueNode.innerText

proc parseBoostSqlXml*(xmlContent: string): SqlResultSet =
  ## Parse Boost serialization XML containing SQL query results
  ## Returns a SqlResultSet with column names and row data
  result = SqlResultSet(columns: @[], rows: @[])
  
  # Parse the XML
  let doc = parseXml(xmlContent)
  
  # Extract column names from resultfieldnames
  let fieldNamesNode = doc.child("resultfieldnames")
  if fieldNamesNode != nil:
    for item in fieldNamesNode.findAll("item"):
      result.columns.add(item.innerText)
  
  # Extract row data from resultrows
  let resultRowsNode = doc.child("resultrows")
  if resultRowsNode != nil:
    # Find all row items (first level items)
    for rowItem in resultRowsNode.findAll("item"):
      var rowData: seq[string] = @[]
      
      # Get the second node which contains the columns
      let columnsContainer = rowItem.child("second")
      if columnsContainer != nil:
        # Find all column items within this row
        for colItem in columnsContainer.findAll("item"):
          let value = extractValue(colItem)
          rowData.add(value)
      
      if rowData.len > 0:
        result.rows.add(rowData)

proc parseBoostSqlXmlFile*(filename: string): SqlResultSet =
  ## Parse Boost serialization XML from a file
  let content = readFile(filename)
  return parseBoostSqlXml(content)

proc toTable*(resultSet: SqlResultSet): Table[string, seq[string]] =
  ## Convert SqlResultSet to a Table with column names as keys
  result = initTable[string, seq[string]]()
  for i, col in resultSet.columns:
    var columnData: seq[string] = @[]
    for row in resultSet.rows:
      if i < row.len:
        columnData.add(row[i])
    result[col] = columnData

proc `$`*(table: Table[string, seq[string]]): string =
  ## Convert a Table[string, seq[string]] to a formatted table string
  if table.len == 0:
    return "Empty table"
  
  # Get column names and determine the maximum number of rows
  let columns = toSeq(table.keys)
  let maxRows = if columns.len > 0: max(columns.mapIt(table[it].len)) else: 0
  
  if maxRows == 0:
    return "Table has no data rows"
  
  var lines: seq[string] = @[]
  
  # Calculate column widths
  var widths: seq[int] = @[]
  for col in columns:
    var maxWidth = col.len
    for value in table[col]:
      if value.len > maxWidth:
        maxWidth = value.len
    widths.add(maxWidth + 2)  # Add padding
  
  # Add header
  lines.add("┌" & widths.mapIt("─".repeat(it)).join("┬") & "┐")
  var headerLine = "│"
  for i, col in columns:
    headerLine.add(" " & col.alignLeft(widths[i] - 2) & " │")
  lines.add(headerLine)
  lines.add("├" & widths.mapIt("─".repeat(it)).join("┼") & "┤")
  
  # Add rows
  for rowIdx in 0..<maxRows:
    var rowLine = "│"
    for i, col in columns:
      let value = if rowIdx < table[col].len: table[col][rowIdx] else: ""
      rowLine.add(" " & value.alignLeft(widths[i] - 2) & " │")
    lines.add(rowLine)
  
  lines.add("└" & widths.mapIt("─".repeat(it)).join("┴") & "┘")
  
  return lines.join("\n")
