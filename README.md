# OlyVIADownloader

A Nim-based barebones CLI tool for downloading files from OlyVIA 2.5 imaging servers.

## Installation

```bash
# Install dependencies and build
nimble install -d:release

# Build without installing
nimble build -d:release
```

## Usage

```bash
./OlyVIADownloader -a server:port -db database_name -u username -p password -d download_directory
```

Or with environment variables:

```bash
export ADDRESS="server:port"
export DATABASE="database_name"
export USERNAME="username"
export PASSWORD="password"
export DIRECTORY="download_directory"  # Optional, defaults to "download"
./OlyVIADownloader
```

## License

MIT License
