import argparse
import os, options, asyncdispatch
import downloader

when isMainModule:
  var p = newParser:
    option("-a", "--address", "Address:port of the OlyVIA server")
    option("-db", "--database", "Database name")
    option("-u", "--username", "Username for the OlyVIA server")
    option("-p", "--password", "Password for the OlyVIA server")
    option("-d", "--directory", default=some("download"), help="Directory to save the downloaded files")
    option("-f", "--file", "Optional: File to download")

  let args = p.parse(commandLineParams())

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

  var file = if args.file != "":
    args.file
  elif getEnv("FILE") != "":
    getEnv("FILE")
  else:
    ""

  var addresssplit = address.split(":")
  if addresssplit.len != 2:
    raise newException(ValueError, "Address must be in the format address:port")
  
  waitFor downloader(addresssplit[0], addresssplit[1].parseInt, username, password, database, directory, file)