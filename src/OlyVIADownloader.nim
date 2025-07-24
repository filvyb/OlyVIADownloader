import argparse
import os, options, asyncdispatch, strutils
import downloader, structs

const AppVersion = block:
  const nimbleFile = staticRead("../OlyVIADownloader.nimble")
  var version = "unknown"
  for line in nimbleFile.splitLines():
    let cleanLine = line.strip()
    if cleanLine.startsWith("version") and "=" in cleanLine:
      let parts = cleanLine.split("=", 1)
      if parts.len == 2:
        let versionPart = parts[1].strip()
        version = versionPart.strip(chars = {'"', '\''})
        break
  version

when isMainModule:
  var p = newParser:
    option("-a", "--address", "Address:port of the OlyVIA server")
    option("-db", "--database", "Database name")
    option("-u", "--username", "Username for the OlyVIA server")
    option("-p", "--password", "Password for the OlyVIA server")
    option("-d", "--directory", default=some("download"), help="Directory to save the downloaded files")
    option("-f", "--filter", default=some(""), help="Download files that contain this filter in their name")
    flag("-l", "--list", help="List available files instead of downloading them")
    flag("-v", "--version", help="Show version information")

  let args = p.parse(commandLineParams())

  # Handle version flag
  if args.version:
    echo "OlyVIADownloader version ", AppVersion
    quit(0)

  var address = if args.address != "":
    args.address
  elif getEnv("ADDRESS") != "":
    getEnv("ADDRESS")
  else:
    raise newException(ValueError, "Address is required")

  var database = if args.database != "":
    args.database
  elif getEnv("DATABASE") != "":
    getEnv("DATABASE")
  else:
    raise newException(ValueError, "Database is required")

  var username = if args.username != "":
    args.username
  elif getEnv("USERNAME") != "":
    getEnv("USERNAME")
  else:
    raise newException(ValueError, "Username is required")

  var password = if args.password != "":
    args.password
  elif getEnv("PASSWORD") != "":
    getEnv("PASSWORD")
  else:
    raise newException(ValueError, "Password is required")

  var directory = if args.directory != "":
    args.directory
  elif getEnv("DIRECTORY") != "":
    getEnv("DIRECTORY")
  else:
    "download"
  
  var filter = if args.filter != "":
    args.filter
  elif getEnv("FILTER") != "":
    getEnv("FILTER")
  else:
    ""
  
  var listFiles = args.list
  if getEnv("LIST") != "":
    listFiles = getEnv("LIST").parseBool

  var addresssplit = address.split(":")
  if addresssplit.len != 2:
    raise newException(ValueError, "Address must be in the format address:port")

  let downloaderConfig = DownloaderConfig(
    address: addresssplit[0],
    port: addresssplit[1].parseInt,
    username: username,
    password: password,
    database: database,
    directory: directory,
    filter: filter,
    list: listFiles
  )

  waitFor downloader(downloaderConfig)
