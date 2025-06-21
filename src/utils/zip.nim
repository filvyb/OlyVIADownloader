import std/[base64, encodings, os, sequtils, strutils, tempfiles]
import zippy/[deflate, crc, ziparchives]
import ../structs
import boost

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
  let boostText = boostText.replace("\n", "").strip()
  let utf16 = convert(boostText, "utf-16", "utf-8")
  var b64 = base64.encode(utf16)
  b64 = b64.replace("\n", "\r\n")
  if b64.endsWith("\r\n"):
    b64.setLen(b64.len - 2)
  b64.add("\x00\x00")

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

  result = zip.toSeq.mapIt(byte(it))

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

proc parseBoostSqlXmlFromZip*(zipData: seq[byte]): SqlResultSet =
  ## Parse Boost SQL XML from a ZIP archive
  ## Returns a SqlResultSet with column names and row data
  var boostXml = digUpBoostBin(zipData)
  if "<boost_serialization" notin boostXml:
    raise newException(ValueError, "Invalid Boost serialization XML format")
  if "</boost_serialization>" notin boostXml:
    boostXml &= "</boost_serialization>"  # Ensure it is well-formed XML

  return parseBoostSqlXml(boostXml)

when isMainModule:
  # Testing the boostBinToZip function
  let tmpSeq = @[
    0x50'u8, 0x4b, 0x03, 0x04, 0x14,
    0x00, 0x00, 0x00, 0x08, 0x00, 0xcd, 0x84, 0x32, 0x59, 0x8d, 0x78, 0x7a, 0xcf, 0xd7, 0x00, 0x00,
    0x00, 0xde, 0x01, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69,
    0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69, 0x6e, 0x7d, 0x91, 0x41, 0x6b, 0x83, 0x40, 0x10, 0x85, 0xef,
    0x82, 0xff, 0x25, 0xd2, 0x26, 0x78, 0xfd, 0x36, 0xda, 0x55, 0x88, 0x94, 0x3d, 0x14, 0xaa, 0xb7,
    0x24, 0x85, 0x0d, 0x18, 0x68, 0x0f, 0x21, 0xab, 0xfd, 0xf5, 0x9d, 0xd1, 0x36, 0x2a, 0x14, 0x4f,
    0xc3, 0x7b, 0x33, 0x3b, 0xef, 0xbd, 0xd9, 0xca, 0xd3, 0xb3, 0x87, 0x73, 0x30, 0x57, 0x8a, 0x92,
    0xa3, 0x33, 0x17, 0x6c, 0xd0, 0xba, 0xc3, 0xe6, 0x7c, 0x60, 0xbe, 0xb0, 0x29, 0x27, 0xcf, 0x8e,
    0xec, 0x93, 0xda, 0x99, 0x1e, 0x5b, 0x71, 0x54, 0xbe, 0xa8, 0x69, 0x1c, 0x9e, 0xcc, 0x51, 0x42,
    0x88, 0x23, 0xdd, 0x54, 0xa1, 0x4c, 0x35, 0x30, 0x13, 0x46, 0x71, 0xa7, 0xf8, 0xe0, 0xd8, 0xfe,
    0xbe, 0xb8, 0x91, 0xe5, 0x8f, 0xb9, 0xe5, 0x7c, 0x4e, 0x1c, 0x09, 0xd3, 0xff, 0xb7, 0x61, 0x86,
    0xc7, 0x17, 0x83, 0x87, 0xb7, 0xa9, 0xef, 0xc6, 0x0d, 0xa3, 0x42, 0xcb, 0xab, 0xcc, 0x8b, 0xb7,
    0xbf, 0x0d, 0xc2, 0x94, 0x43, 0x2a, 0xc7, 0x7b, 0x30, 0x77, 0xac, 0xa4, 0xf6, 0x92, 0x5e, 0x52,
    0x49, 0xda, 0x0d, 0x45, 0xcb, 0x19, 0xc1, 0x2f, 0xa9, 0xd6, 0x8b, 0x5e, 0x45, 0x52, 0xdf, 0xa6,
    0xcd, 0xe5, 0xe8, 0xad, 0x9b, 0xbb, 0x15, 0x9c, 0xcc, 0x27, 0x96, 0x69, 0xfd, 0xdc, 0xeb, 0x93,
    0x56, 0xb9, 0x6f, 0xab, 0x0a, 0x27, 0x51, 0x10, 0x6f, 0xf6, 0x99, 0x26, 0x98, 0x44, 0x2f, 0xde,
    0x84, 0x75, 0xed, 0x75, 0x65, 0xbd, 0x9b, 0x74, 0xbe, 0xf5, 0x07, 0xe2, 0xe8, 0x07, 0x50, 0x4b,
    0x01, 0x02, 0x2e, 0x00, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00, 0xcd, 0x84, 0x32, 0x59, 0x8d, 0x78,
    0x7a, 0xcf, 0xd7, 0x00, 0x00, 0x00, 0xde, 0x01, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0xb4, 0x81, 0x00, 0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69,
    0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69, 0x6e, 0x50, 0x4b, 0x05, 0x06, 0x00, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x3c, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00
  ]
  echo digUpBoostBin(tmpSeq)
  let boostText = """
22 serialization::archive 4 0 0
1 0 0 0 1 -94 -1 0 0 0 1 2 0 0 1 0 0 0 1 5 1 1
"""
  let compressedBinary = boostBinToZip(boostText)
  echo "Hexadecimal: ", compressedBinary.mapIt(it.toHex(2)).join("")