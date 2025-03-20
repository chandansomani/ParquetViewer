using CsvHelper.Configuration;
using CsvHelper;
using ParquetViewer.Engine;
using ParquetViewer.Engine.Exceptions;
using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ParquetDuplicateFinder
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var options = CommandLineParser.Parse(args);
                if (options == null) return; // Help or invalid arguments
                if (options != null)
                {
                    CommandLineParser.PrintCurrentConfiguration(options);
                }

                DataTable dataTable;

                if (options.IsCsv)
                {
                    dataTable = CsvOperations.ReadCsv(options.FilePath, options.Delimiter, options.HasHeader);
                }
                else
                {
                    var parquetEngine = await ParquetOperations.OpenFileOrFolderAsync(options.FilePath);
                    var fieldsToCheck = ParquetOperations.GetFieldsToCheck(parquetEngine, options.Fields, options.ColumnIndices);
                    dataTable = await ParquetOperations.ReadDataAsync(parquetEngine, fieldsToCheck);
                }

                if (options.PrintData)
                {
                    PrintData.DisplayData(dataTable, options.RowLimit);
                }

                if (options.FindDuplicates)
                {
                    if (options.Fields.Count == 0)
                    {
                        Console.WriteLine("No fields specified. Using all columns.");
                        options.Fields = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
                    }
                    Console.WriteLine($"Fields used for duplicate check [{options.Fields.Count}]: {string.Join(", ", options.Fields)}");

                    DuplicateFinder.FindAndDisplayDuplicates(dataTable, options.Fields, options.Verbose, options.Limit);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                if (ex is AllFilesSkippedException afse)
                {
                    Console.WriteLine($"Details: {afse.Message}");
                }
                else if (ex is SomeFilesSkippedException sfse)
                {
                    Console.WriteLine($"Details: {sfse.Message}");
                }
                else if (ex is FileReadException fre)
                {
                    Console.WriteLine($"Details: {fre.Message}");
                }
                else if (ex is MultipleSchemasFoundException msfe)
                {
                    Console.WriteLine($"Details: {msfe.Message}");
                }
            }
        }
    }

    // Command-line argument parsing
    static class CommandLineParser
    {
        public static Options Parse(string[] args)
        {
            if (args.Length < 1)
            {
                PrintUsage();
                return null;
            }

            var options = new Options { FilePath = args[0] };

            var validArgs = new HashSet<string>
            {
                "--csv", "--delimiter", "--header", "-f", "--fields",
                "-c", "--columns", "-v", "--verbose", "-l", "--limit",
                "-d", "--findDuplicates", "-pf", "--printData", "-h", "--help"
            };

            for (int i = 1; i < args.Length; i++)
            {
                string arg = args[i];

                switch (args[i])
                {
                    case "--csv":
                        options.IsCsv = true;
                        break;
                    case "--delimiter":
                        if (i + 1 < args.Length)
                        {
                            options.Delimiter = args[i + 1][0];
                            i++;
                        }
                        break;
                    case "--header":
                        options.HasHeader = true;
                        break;
                    case "-f":
                    case "--fields":
                        if (i + 1 < args.Length)
                        {
                            options.Fields = args[i + 1].Split(',').Select(f => f.Trim()).ToList();
                            i++;
                        }
                        break;
                    case "-c":
                    case "--columns":
                        if (i + 1 < args.Length)
                        {
                            options.ColumnIndices = args[i + 1].Split(',')
                                .Where(n => int.TryParse(n, out _))
                                .Select(int.Parse)
                                .ToList();
                            i++;
                        }
                        break;
                    case "-v":
                    case "--verbose":
                        options.Verbose = true;
                        break;
                    case "-l":
                    case "--limit":
                        if (i + 1 < args.Length && int.TryParse(args[i + 1], out int limit))
                        {
                            options.Limit = limit;
                            i++;
                        }
                        break;
                    case "-d":
                    case "--findDuplicates":
                        options.FindDuplicates = true;
                        break;
                    case "-pf":
                    case "--printData":
                        options.PrintData = true;
                        if (i + 1 < args.Length && long.TryParse(args[i + 1], out long rowLimit))
                        {
                            options.RowLimit = rowLimit;
                            i++;
                        }
                        else
                        {
                            options.RowLimit = -1;
                        }
                        break;
                    case "-h":
                    case "--help":
                        PrintUsage();
                        return null;
                    default:
                        if (!validArgs.Contains(arg))
                        {
                            Console.WriteLine($"Invalid argument: {arg}");
                            PrintUsage();
                            return null;
                        }
                        break;

                }
            }

            return options;
        }

        public static void PrintCurrentConfiguration(Options options)
        {
            Console.WriteLine($@"
                ===== Command Line Configuration =====
                File: {options.FilePath}
                Mode: {(options.IsCsv ? "CSV" : "Parquet")} | Delimiter: '{options.Delimiter}' | Header: {options.HasHeader}
                Fields: {(options.Fields.Count > 0 ? string.Join(", ", options.Fields) : "All Columns")}
                Columns: {(options.ColumnIndices.Count > 0 ? string.Join(", ", options.ColumnIndices) : "N/A")}
                Verbose: {options.Verbose} | Duplicates: {options.FindDuplicates} | Limit: {(options.Limit > 0 ? options.Limit : "Unlimited")}
                Print Data: {options.PrintData} | Row Limit: {(options.RowLimit > 0 ? options.RowLimit : "All")}
                ====================================
            ");
        }

        private static void PrintUsage()
        {
            Console.WriteLine("ParquetDuplicateFinder - Find duplicate records in Parquet or CSV files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  ParquetDuplicateFinder <file_or_folder_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  --csv                            Process a CSV file instead of Parquet");
            Console.WriteLine("  --delimiter <char>                Specify CSV delimiter (default: ',')");
            Console.WriteLine("  --header                         Treat first row as CSV header");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Specify fields to check for duplicates");
            Console.WriteLine("  -c, --columns <index1,index2,...> Specify column indices for duplicates");
            Console.WriteLine("  -v, --verbose                     Show all fields for duplicate records");
            Console.WriteLine("  -l, --limit <number>              Limit number of duplicate groups to display");
            Console.WriteLine("  -d,                               Find and Display Duplicates");
            Console.WriteLine("  -pf,                              Display Parquet/CSV Data");
            Console.WriteLine("  -h, --help                        Show this help message");
        }
    }

    // Options class to hold command-line arguments
    class Options
    {
        public string FilePath { get; set; }
        public List<string> Fields { get; set; } = new List<string>();
        public List<int> ColumnIndices { get; set; } = new List<int>();
        public bool Verbose { get; set; }
        public int Limit { get; set; } = -1;
        public bool FindDuplicates { get; set; }
        public bool PrintData { get; set; }
        public long RowLimit { get; set; }
        public bool IsCsv { get; set; }
        public char Delimiter { get; set; } = ',';
        public bool HasHeader { get; set; } = true;
    }


    static class CsvOperations
    {
        public static DataTable ReadCsv(string filePath, char delimiter, bool hasHeader)
        {
            Console.WriteLine($"Using delimiter: '{delimiter}'");

            var dataTable = new DataTable();
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.ToString(),
                HasHeaderRecord = hasHeader,
                IgnoreBlankLines = true,
                TrimOptions = TrimOptions.Trim,
            };

            using var reader = new StreamReader(filePath);
            using var csv = new CsvReader(reader, config);

            if (hasHeader)
            {
                csv.Read();
                csv.ReadHeader();
                foreach (var header in csv.HeaderRecord)
                {
                    dataTable.Columns.Add(header);
                }
                //Console.WriteLine("CSV Columns: " + string.Join(" | ", csv.HeaderRecord));
            }

            int rowCount = 0;
            while (csv.Read())
            {
                var row = dataTable.NewRow();
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    row[i] = csv.GetField(i)?.Trim() ?? "NULL";
                }
                dataTable.Rows.Add(row);

                // Debug: Print row data
                //Console.WriteLine("Row " + (rowCount + 1) + ": " + string.Join(" | ", row.ItemArray));
                rowCount++;
            }

            Console.WriteLine("Total rows in CSV: " + rowCount);

            return dataTable;
        }
    }

    // Parquet file operations
    static class ParquetOperations
    {
        public static async Task<ParquetEngine> OpenFileOrFolderAsync(string filePath)
        {
            Console.WriteLine($"Opening {filePath}...");
            return await ParquetEngine.OpenFileOrFolderAsync(filePath, CancellationToken.None);
        }

        public static List<string> GetFieldsToCheck(ParquetEngine parquetEngine, List<string> fields, List<int> columnIndices)
        {
            var availableFields = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();

            if (fields.Count == 0 && columnIndices.Count == 0)
            {
                Console.WriteLine("No fields or columns specified, using all available fields for duplicate checking.");
                return availableFields;
            }

            if (columnIndices.Count > 0)
            {
                var fieldsFromIndices = columnIndices
                    .Where(i => i >= 0 && i < availableFields.Count)
                    .Select(i => availableFields[i])
                    .ToList();

                if (fieldsFromIndices.Count == 0)
                {
                    Console.WriteLine("Warning: No valid column indices provided.");
                }

                return fieldsFromIndices;
            }

            var validFields = fields.Where(f => availableFields.Contains(f, StringComparer.OrdinalIgnoreCase)).ToList();
            var invalidFields = fields.Except(validFields, StringComparer.OrdinalIgnoreCase).ToList();

            if (invalidFields.Any())
            {
                Console.WriteLine($"Warning: The following fields do not exist in the Parquet file: {string.Join(", ", invalidFields)}");
            }

            return validFields;
        }

        public static async Task<DataTable> ReadDataAsync(ParquetEngine parquetEngine, List<string> fieldsToCheck)
        {
            Console.WriteLine($"Reading data with fields: {string.Join(", ", fieldsToCheck)}");
            var loadResult = await parquetEngine.ReadRowsAsync(
                fieldsToCheck,
                0,
                (int)parquetEngine.RecordCount,
                CancellationToken.None,
                null);

            return loadResult.Invoke(false);
        }
    }

    // Duplicate finding logic
    static class DuplicateFinder
    {
        public static void FindAndDisplayDuplicates(DataTable dataTable, List<string> fieldsToCheck, bool verbose, int limit)
        {
            Console.WriteLine("Searching for duplicates...");

            // Group rows by the specified fields
            var duplicateGroups = new Dictionary<string, List<DataRow>>();

            // Use Parallel.ForEach for concurrent processing
            Parallel.ForEach(dataTable.AsEnumerable(), row =>
            {
                var key = CreateKey(row, fieldsToCheck);

                lock (duplicateGroups) // Ensure thread-safe access to the dictionary
                {
                    if (!duplicateGroups.ContainsKey(key))
                    {
                        duplicateGroups[key] = new List<DataRow>();
                    }
                    duplicateGroups[key].Add(row);
                }
            });

            // Filter groups with duplicates
            var duplicateGroupsFiltered = duplicateGroups
                .Where(g => g.Value.Count > 1)
                .ToList();

            if (duplicateGroupsFiltered.Count == 0)
            {
                Console.WriteLine("No duplicates found.");
                return;
            }

            Console.WriteLine($"Found {duplicateGroupsFiltered.Count} duplicate groups.");

            // Apply limit if specified
            if (limit > 0 && duplicateGroupsFiltered.Count > limit)
            {
                Console.WriteLine($"Showing first {limit} duplicate groups (use --limit to adjust).");
                duplicateGroupsFiltered = duplicateGroupsFiltered.Take(limit).ToList();
            }

            // Display duplicates
            int groupNumber = 1;
            foreach (var group in duplicateGroupsFiltered)
            {
                Console.WriteLine($"\nDuplicate Group #{groupNumber++} ({group.Value.Count} records):");

                if (verbose)
                {
                    Console.Write($"{"#####",5}:");
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        Console.Write($"{column.ColumnName,-36} | ");
                    }
                    Console.WriteLine("");
                    int recordNumber = 1;
                    foreach (var row in group.Value)
                    {
                        Console.Write($"{recordNumber++,5}:");
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            var value = row[column];
                            Console.Write($"{value?.ToString()[..36] ?? "NULL", -36} | ");
                        }
                        Console.WriteLine("");
                    }
                }
                else
                {
                    foreach (var field in fieldsToCheck)
                    {
                        Console.Write($"{field,-36} | ");
                    }
                    Console.WriteLine();
                    foreach (var field in fieldsToCheck)
                    {
                        Console.Write($"{group.Value[0][field].ToString()[..36], -36} | ");
                    }
                    Console.WriteLine();
                }
            }

            // Summary
            int totalDuplicates = duplicateGroupsFiltered.Sum(g => g.Value.Count - 1);
            Console.WriteLine($"\nSummary: Found {totalDuplicates} duplicate records in {duplicateGroupsFiltered.Count} groups.");
        }

        private static string CreateKey(DataRow row, List<string> fieldsToCheck)
        {
            // Concatenate the field values into a single string
            var keyBuilder = new StringBuilder();
            foreach (var field in fieldsToCheck)
            {
                keyBuilder.Append(row[field]?.ToString() ?? "NULL");
                keyBuilder.Append("|"); // Separator
            }

            // Compute the SHA256 hash of the concatenated string
            using (var sha256 = SHA256.Create())
            {
                var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(keyBuilder.ToString()));
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }
    }

    static class PrintData
    {
        public static void DisplayData(DataTable dataTable)
        {
            foreach (DataColumn column in dataTable.Columns)
            {
                Console.Write($"{column.ColumnName,-36} | ");
            }
            Console.WriteLine();

            // Print rows
            foreach (DataRow row in dataTable.Rows)
            {
                foreach (DataColumn column in dataTable.Columns)
                {
                    var value = row[column]?.ToString() ?? "NULL";
                    if (value.Length > 36)
                    {
                        value = value.Substring(0, 36);
                    }
                    Console.Write($"{value,-36} | ");
                }
                Console.WriteLine();
            }
        }

        public static void DisplayData(DataTable dataTable, long limit)
        {
            foreach (DataColumn column in dataTable.Columns)
            {
                Console.Write($"{column.ColumnName,-36} | ");
            }
            Console.WriteLine(new string('-' ,dataTable.Columns.Count*(36+3)));

            long rowNo = 0;
            foreach (DataRow row in dataTable.Rows)
            {
                if(limit == -1 || rowNo < limit)
                {
                    foreach (DataColumn column in dataTable.Columns)
                    {
                        var value = row[column]?.ToString() ?? "NULL";
                        if (value.Length > 36)
                        {
                            value = value.Substring(0, 36);
                        }
                        Console.Write($"{value,-36} | ");
                    }
                    Console.WriteLine();
                    rowNo++;
                }
            }
        }
    }

    // Statistics display logic
    static class ParquetStatistics
    {
        public static void Display(ParquetEngine parquetEngine, string filePath)
        {
            Console.WriteLine("\n===== PARQUET FILE STATISTICS =====");
            Console.WriteLine($"Type: {(Directory.Exists(filePath) ? "Folder" : "File")}");
            Console.WriteLine($"Path: {filePath}");
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
            //Console.WriteLine($"Number of Partitions: {parquetEngine.NumberOfPartitions}");
            //Console.WriteLine($"Number of Row Groups: {parquetEngine.ThriftMetadata.RowGroups.Count}");

            var dataFields = parquetEngine.Schema.DataFields;
            Console.WriteLine($"\nColumns: {dataFields.Length}");
            Console.WriteLine("\nColumn Details:");
            Console.WriteLine(new string('-', 80));
            Console.WriteLine($"{"Position",-8} | {"Name",-30} | {"SchemaType",-20}");
            Console.WriteLine(new string('-', 80));

            for (int i = 0; i < dataFields.Length; i++)
            {
                var field = dataFields[i];
                Console.WriteLine($"{i,-8} | {field.Name,-30} | {field.SchemaType,-20}");
            }

            Console.WriteLine(new string('-', 80));

            var schemaTypeCounts = dataFields.GroupBy(f => f.SchemaType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);

            Console.WriteLine("\nSchema Type Summary:");
            foreach (var typeCount in schemaTypeCounts)
            {
                Console.WriteLine($"  {typeCount.Type,-20}: {typeCount.Count} column(s)");
            }

            if (File.Exists(filePath))
            {
                var fileInfo = new FileInfo(filePath);
                Console.WriteLine($"\nFile Size: {FormatFileSize(fileInfo.Length)}");
            }
            else if (Directory.Exists(filePath))
            {
                var dirInfo = new DirectoryInfo(filePath);
                var files = dirInfo.GetFiles("*.parquet", SearchOption.AllDirectories);
                long totalSize = files.Sum(f => f.Length);
                Console.WriteLine($"\nTotal Size of Parquet Files: {FormatFileSize(totalSize)}");
                Console.WriteLine($"Number of Parquet Files: {files.Length}");
            }

            Console.WriteLine("\n===================================\n");
        }

        private static string FormatFileSize(long bytes)
        {
            string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
            int counter = 0;
            decimal number = bytes;

            while (Math.Round(number / 1024) >= 1)
            {
                number = number / 1024;
                counter++;
            }

            return $"{number:n2} {suffixes[counter]}";
        }
    }
}