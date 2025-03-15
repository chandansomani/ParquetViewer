# Parquet File Processor

A command-line utility for processing, viewing, and analyzing Parquet file metadata and structure, find Duplicates with specified columns.

## Overview

Parquet File Processor provides a simple way to inspect and work with Parquet files, displaying key metadata including schema information, column types, record counts, and file size statistics. It supports both individual Parquet files and directories containing multiple Parquet files.

## Credits

This project is forked from [ParquetViewer](https://github.com/mukunku/ParquetViewer/) by [Mukunku](https://github.com/mukunku). The original repository provided the foundation for this expanded command-line tool.

## Features

- Display basic Parquet file metadata
- View column schemas and types
- Show record count and partition information 
- Analyze row group structure
- Provide data type distribution summary
- Calculate storage size information
- Process both individual files and directories of Parquet files
- Find Duplicates present in Parquet files, with specfied columns.

## Command-line Usage

```
ParquetFileProcessor [options] <file-or-directory-path>

Options:
  -s, --stats       Display statistics about the Parquet file(s)
  -m, --metadata    Show detailed metadata
  -o, --output      Specify output format (console, json, csv)
  -h, --help        Show help information
```

## Example Usage

```bash
# Display statistics for a single file
ParquetFileProcessor --stats /path/to/data.parquet

# Process all parquet files in a directory
ParquetFileProcessor --stats /path/to/parquet/directory/

# Export metadata as JSON
ParquetFileProcessor --metadata --output json /path/to/data.parquet > metadata.json
```

## Output Example

```
===== PARQUET FILE STATISTICS =====
Type: File
Path: C:\Data\sample.parquet
Total Records: 10,000
Number of Partitions: 1
Number of Row Groups: 1

Columns: 5

Column Details:
--------------------------------------------------------------------------------
Position | Name                           | SchemaType          
--------------------------------------------------------------------------------
0        | id                             | Data                
1        | name                           | Data                
2        | timestamp                      | Data                
3        | value                          | Data                
4        | attributes                     | Struct              
--------------------------------------------------------------------------------

Schema Type Summary:
  Data                : 4 column(s)
  Struct              : 1 column(s)

File Size: 1.25 MB

===================================
```

## Implementation Details

The processor utilizes the Parquet.Schema namespace to extract and display schema information. It builds upon the foundation provided by ParquetViewer, extending it with command-line functionality and additional processing features.

## Requirements

- .NET 6.0 or higher
- Parquet library dependencies

## Installation

```bash
# Install from NuGet
dotnet tool install -g ParquetFileProcessor

# Or build from source
git clone https://github.com/yourusername/ParquetFileProcessor.git
cd ParquetFileProcessor
dotnet build
dotnet publish -c Release
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the same license as the original ParquetViewer repository.
