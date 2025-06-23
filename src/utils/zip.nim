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
    0x50'u8, 0x4b, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00,
    0xd0, 0x84, 0x32, 0x59, 0xce, 0xc6, 0xf7, 0x2c, 0x99, 0x00, 0x00, 0x00, 0x34, 0x01, 0x00, 0x00,
    0x0e, 0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69, 0x7a, 0x65, 0x64, 0x2e, 0x62,
    0x69, 0x6e, 0x6d, 0x8e, 0x4d, 0x0b, 0x82, 0x40, 0x18, 0x84, 0x3d, 0x0b, 0xfe, 0x97, 0x2e, 0x49,
    0x97, 0x0e, 0xcf, 0xba, 0xb2, 0x0a, 0x69, 0xec, 0xa1, 0x83, 0xdd, 0xcc, 0x60, 0x83, 0x82, 0x3a,
    0x04, 0x9b, 0xfd, 0xfa, 0x7c, 0x55, 0xfc, 0x80, 0x4e, 0xcb, 0xcc, 0xbc, 0xf3, 0xec, 0x14, 0x8e,
    0x96, 0x04, 0x1a, 0xaf, 0x1e, 0x64, 0x39, 0xb5, 0x55, 0x37, 0x8c, 0x97, 0x37, 0xc6, 0xa4, 0x5c,
    0x51, 0x2f, 0xcc, 0x8e, 0x8b, 0x23, 0x46, 0x3f, 0xa9, 0xac, 0x6a, 0x31, 0x05, 0xb5, 0xf8, 0x59,
    0xc5, 0xd9, 0xe2, 0xd0, 0x96, 0x1c, 0x7c, 0x14, 0x0a, 0xa9, 0x40, 0x9c, 0xbc, 0x77, 0x66, 0x8d,
    0xe8, 0x8f, 0xe8, 0x83, 0x65, 0x3b, 0x36, 0xde, 0xe8, 0x74, 0xba, 0x5b, 0xdf, 0xa7, 0x44, 0x61,
    0xe7, 0xb4, 0xff, 0x08, 0x0b, 0x3d, 0x34, 0xfa, 0x0d, 0xa7, 0x39, 0xb7, 0x03, 0x61, 0xf8, 0xe1,
    0xce, 0xb1, 0xd3, 0xb2, 0x6d, 0x33, 0x26, 0xb2, 0x4d, 0x25, 0x2b, 0xa2, 0x9f, 0x1a, 0x0b, 0x62,
    0x33, 0xe5, 0xa5, 0xe5, 0x8b, 0x76, 0x94, 0xb0, 0x0f, 0x82, 0x1f, 0x50, 0x4b, 0x01, 0x02, 0x2e,
    0x00, 0x14, 0x00, 0x00, 0x00, 0x08, 0x00, 0xd0, 0x84, 0x32, 0x59, 0xce, 0xc6, 0xf7, 0x2c, 0x99,
    0x00, 0x00, 0x00, 0x34, 0x01, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x20, 0x00, 0xb4, 0x81, 0x00, 0x00, 0x00, 0x00, 0x73, 0x65, 0x72, 0x69, 0x61, 0x6c, 0x69,
    0x7a, 0x65, 0x64, 0x2e, 0x62, 0x69, 0x6e, 0x50, 0x4b, 0x05, 0x06, 0x00, 0x00, 0x00, 0x00, 0x01,
    0x00, 0x01, 0x00, 0x3c, 0x00, 0x00, 0x00, 0xc5, 0x00, 0x00, 0x00, 0x00, 0x00
  ]
  echo digUpBoostBin(tmpSeq)
  let boostText = """
22 serialization::archive 4 0 0
1 0 0 0 1 -94 -1 0 0 0 1 2 0 0 1 0 0 0 1 5 1 1
"""
  let compressedBinary = boostBinToZip(boostText)
  echo "Hexadecimal: ", compressedBinary.mapIt(it.toHex(2)).join("")