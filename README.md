# OlyVIADownloader

A Nim-based barebones CLI tool for downloading .vsi files from OlyVIA 2.5 imaging servers.

## Installation

```bash
# Install dependencies and build
nimble install -d:release -d:lto

# Build without installing
nimble build -d:release -d:lto
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

Downloaded .vsi files can be viewed with [ImageJ](https://imagej.net/formats/olympus) or viewed and processed with [bftools](https://bio-formats.readthedocs.io/en/stable/users/comlinetools/index.html)

## License

MIT License
