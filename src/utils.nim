import std/[base64, encodings, os, sequtils, strutils, tempfiles, xmlparser, xmltree, tables]
import zippy/[deflate, crc, ziparchives]
import structs

const b64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

proc u16le(n: uint16): string =
  result = newString(2)
  result[0] = char(n and 0xFF)
  result[1] = char((n shr 8) and 0xFF)

proc u32le(n: uint32): string =
  result = newString(4)
  result[0] = char(n and 0xFF)
  result[1] = char((n shr 8) and 0xFF)
  result[2] = char((n shr 16) and 0xFF)
  result[3] = char((n shr 24) and 0xFF)

proc boostBinToZip*(boostText: string, filename = "serialized.bin"): seq[byte] =
  ## Converts a Boost text archive string to a compressed binary in ZIP format.
  ## Parameters:
  ##   boostText - The Boost text archive string
  ## Returns compressed binary in ZIP format.
  let utf16 = convert(boostText, "utf-16", "utf-8")
  let b64 = base64.encode(utf16)

  var deflated = ""
  deflate(deflated, cast[ptr UncheckedArray[uint8]](b64.cstring), b64.len, 8)

  let crc        = crc32(b64)
  let compSize   = uint32(deflated.len)
  let uncompSize = uint32(b64.len)

  var localHdr = ""
  localHdr.add u32le(0x04034b50)      # Signature "PK\x03\x04"
  localHdr.add u16le(20)              # Version needed to extract = 2.0
  localHdr.add u16le(0)               # General purpose bit‑flag
  localHdr.add u16le(8)               # Compression method = Deflate
  localHdr.add u16le(0)               # File mod time (none)
  localHdr.add u16le(0)               # File mod date (none)
  localHdr.add u32le(crc)             # CRC‑32 over *uncompressed* data
  localHdr.add u32le(compSize)        # compressed size
  localHdr.add u32le(uncompSize)      # uncompressed size
  localHdr.add u16le(uint16(filename.len))  # file‑name length
  localHdr.add u16le(0)               # extra‑field length
  localHdr.add filename               # file‑name

  var central = ""
  central.add u32le(0x02014b50)       # Signature "PK\x01\x02"
  central.add u16le(0x002E)           # Version *made by* (46, host = FAT)
  central.add u16le(20)               # Version needed to extract
  central.add u16le(0)                # GP bit‑flag
  central.add u16le(8)                # Compression = Deflate
  central.add u16le(0)                # Mod time
  central.add u16le(0)                # Mod date
  central.add u32le(crc)
  central.add u32le(compSize)
  central.add u32le(uncompSize)
  central.add u16le(uint16(filename.len))  # file‑name length
  central.add u16le(0)                # extra length
  central.add u16le(0)                # comment length
  central.add u16le(0)                # disk num start
  central.add u16le(0)                # internal attributes
  central.add u32le(0)                # external attributes
  central.add u32le(0)                # offset of local header (we start at 0)
  central.add filename

  var eocd = ""
  eocd.add u32le(0x06054b50)          # Signature "PK\x05\x06"
  eocd.add u16le(0)                   # disk number
  eocd.add u16le(0)                   # disk where central dir starts
  eocd.add u16le(1)                   # # of central dir records on this disk
  eocd.add u16le(1)                   # total # of records
  eocd.add u32le(uint32(central.len)) # size of central directory (bytes)
  eocd.add u32le(uint32(uint32(localHdr.len) + compSize)) # offset to central dir
  eocd.add u16le(0)                   # ZIP file comment length

  var zip = localHdr
  zip.add deflated
  zip.add central
  zip.add eocd

  return zip.toSeq.mapIt(byte(it))

proc unzipArchive*(zipData: seq[byte]): seq[string] =
  ## Unzips a ZIP archive and returns the contained files as strings.
  ## Parameters:
  ##   zipData - The ZIP archive data as a sequence of bytes.
  ## Returns a sequence of strings, each representing a file in the archive.

  let tempFile = genTempPath("zip", ".zip")
  writeFile(tempFile, zipData)
  let archive = openZipArchive(tempFile)


  try:
    for path in archive.walkFiles:
      echo "Extracting: ", path
      let contents = archive.extractFile(path)
      result.add(contents)

  finally:
    archive.close()
    removeFile(tempFile)

proc digUpBoostBin*(zipData: seq[byte]): string =
  ## Extracts a Boost archive file from a ZIP archive.
  ## Parameters:
  ##   zipData - The ZIP archive data as a sequence of bytes.
  ##   filename - The name of the Boost archive file to extract.
  ## Returns the contents of the Boost archive file as a string.

  var file = unzipArchive(zipData)[0]  # Assuming the first file is the Boost archive
  if file == "":
    raise newException(ValueError, "No files found in the ZIP archive")
  file = file.filterIt(it in b64Alphabet).join("")  # Filter out non-Base64 characters
  #file = file.replace("\r\n", "")
  let utf16 = decode(file)
  let boost = convert(utf16, "utf-8", "utf-16")
  return boost

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

proc prettyPrint*(resultSet: SqlResultSet) =
  ## Pretty print the SQL result set as a table
  if resultSet.columns.len == 0:
    echo "No results"
    return
  
  # Calculate column widths
  var widths: seq[int] = @[]
  for i, col in resultSet.columns:
    var maxWidth = col.len
    for row in resultSet.rows:
      if i < row.len and row[i].len > maxWidth:
        maxWidth = row[i].len
    widths.add(maxWidth + 2)
  
  # Print header
  echo "┌", widths.mapIt("─".repeat(it)).join("┬"), "┐"
  stdout.write "│"
  for i, col in resultSet.columns:
    stdout.write " ", col.alignLeft(widths[i] - 2), " │"
  echo ""
  echo "├", widths.mapIt("─".repeat(it)).join("┼"), "┤"
  
  # Print rows
  for row in resultSet.rows:
    stdout.write "│"
    for i, val in row:
      if i < widths.len:
        stdout.write " ", val.alignLeft(widths[i] - 2), " │"
    echo ""
  
  echo "└", widths.mapIt("─".repeat(it)).join("┴"), "┘"

proc parseBoostSqlXmlFromZip*(zipData: seq[byte]): SqlResultSet =
  ## Parse Boost SQL XML from a ZIP archive
  ## Returns a SqlResultSet with column names and row data
  var boostXml = digUpBoostBin(zipData)
  if "<boost_serialization" notin boostXml:
    raise newException(ValueError, "Invalid Boost serialization XML format")
  if "</boost_serialization>" notin boostXml:
    boostXml &= "</boost_serialization>"  # Ensure it is well-formed XML

  return parseBoostSqlXml(boostXml)

proc prettyPrint*(table: Table[string, seq[string]]) =
  ## Pretty print a Table[string, seq[string]] as a formatted table
  if table.len == 0:
    echo "Empty table"
    return
  
  # Get column names and determine the maximum number of rows
  let columns = toSeq(table.keys)
  let maxRows = if columns.len > 0: max(columns.mapIt(table[it].len)) else: 0
  
  if maxRows == 0:
    echo "Table has no data rows"
    return
  
  # Calculate column widths
  var widths: seq[int] = @[]
  for col in columns:
    var maxWidth = col.len
    for value in table[col]:
      if value.len > maxWidth:
        maxWidth = value.len
    widths.add(maxWidth + 2)  # Add padding
  
  # Print header
  echo "┌", widths.mapIt("─".repeat(it)).join("┬"), "┐"
  stdout.write "│"
  for i, col in columns:
    stdout.write " ", col.alignLeft(widths[i] - 2), " │"
  echo ""
  echo "├", widths.mapIt("─".repeat(it)).join("┼"), "┤"
  
  # Print rows
  for rowIdx in 0..<maxRows:
    stdout.write "│"
    for i, col in columns:
      let value = if rowIdx < table[col].len: table[col][rowIdx] else: ""
      stdout.write " ", value.alignLeft(widths[i] - 2), " │"
    echo ""
  
  echo "└", widths.mapIt("─".repeat(it)).join("┴"), "┘"

when isMainModule:
  # Testing the boostBinToZip function
  let boostText = """
22 serialization::archive 4 0 0
1 0 0 0 1 -94 -1 0 0 0 1 2 0 0 1 0 0 0 1 5 1 1"""
  let compressedBinary = boostBinToZip(boostText)
  echo "Hexadecimal: ", compressedBinary.mapIt(it.toHex(2)).join("")
  