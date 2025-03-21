using ParquetViewer.Engine;
using System.Data;
using System.Globalization;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using System.Security.Cryptography;

namespace UnifiedDataProcessor
{
    internal class Program
    {
        static async Task UnifiedParquetProcessor(string[] args)
        {
            try
            {
                var options = CommandLineParser.Parse(args);
                if (options == null) return; // Help or invalid arguments

                // Create appropriate data provider based on file extension
                IDataProvider dataProvider = DataProviderFactory.CreateProvider(options);

                // Show stats if requested
                if (options.ShowStats)
                {
                    await dataProvider.DisplayStatisticsAsync();
                }

                // Get fields to check (columns of interest)
                var fieldsToCheck = await dataProvider.GetFieldsToCheckAsync(options.Fields, options.ColumnIndices);
                if (fieldsToCheck.Count == 0)
                {
                    Console.WriteLine("Error: No valid fields to check for duplicates.");
                    return;
                }

                // Display data if requested
                if (options.PrintData)
                {
                    var data = await dataProvider.ReadDataAsync(fieldsToCheck);
                    if (options.RowLimit > 0)
                        DataDisplayer.DisplayData(data, options.RowLimit);
                    else
                        DataDisplayer.DisplayData(data);
                }

                // Find duplicates if requested
                if (options.FindDuplicates)
                {
                    var data = await dataProvider.ReadDataAsync(fieldsToCheck);
                    DuplicateFinder.FindAndDisplayDuplicates(data, fieldsToCheck, options.Verbose, options.Limit);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Details: {ex.InnerException.Message}");
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

            for (int i = 1; i < args.Length; i++)
            {
                switch (args[i])
                {
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
                                .Select(f => f.Trim())
                                .Where(n => int.TryParse(n, out _))
                                .Select(int.Parse)
                                .ToList();
                            i++;
                        }
                        break;
                    case "-d":
                    case "--delimiter":
                        if (i + 1 < args.Length)
                        {
                            options.Delimiter = args[i + 1] == "\\t" ? "\t" : args[i + 1];
                            i++;
                        }
                        break;
                    case "-h":
                    case "--header":
                        options.HasHeader = true;
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
                    case "-s":
                    case "--stats":
                        options.ShowStats = true;
                        break;
                    case "-pf":
                        options.PrintData = true;
                        if (i + 1 < args.Length && long.TryParse(args[i + 1], out long rowLimit))
                        {
                            options.RowLimit = rowLimit;
                            i++;
                        }
                        break;
                    case "-fd":
                    case "--finddup":
                        options.FindDuplicates = true;
                        break;
                    case "--help":
                        PrintUsage();
                        return null;
                    default:
                        Console.WriteLine($"Unknown option: {args[i]}");
                        PrintUsage();
                        return null;
                }
            }

            return options;
        }

        private static void PrintUsage()
        {
            Console.WriteLine("UnifiedDataProcessor - Find duplicate records in CSV and Parquet files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  UnifiedDataProcessor <file_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Specify fields to check for duplicates (default: all fields)");
            Console.WriteLine("  -c, --columns <col1,col2,...>     Specify column indices to check for duplicates (default: all columns)");
            Console.WriteLine("  -d, --delimiter <char>            Specify CSV delimiter (default: ',', only for CSV files)");
            Console.WriteLine("  -h, --header                      Indicates the CSV file has a header row (only for CSV files)");
            Console.WriteLine("  -v, --verbose                     Show all fields for duplicate records");
            Console.WriteLine("  -l, --limit <number>              Limit number of duplicate groups to display");
            Console.WriteLine("  -s, --stats                       Display statistics about the file");
            Console.WriteLine("  -pf                               Display data (all records)");
            Console.WriteLine("  -pf <number>                      Display first n records from the file");
            Console.WriteLine("  -fd, --finddup                    Find duplicates");
            Console.WriteLine("  --help                            Show this help message");
        }
    }

    // Unified options class
    public class Options
    {
        public string FilePath { get; set; }
        public List<string> Fields { get; set; } = new List<string>();
        public List<int> ColumnIndices { get; set; } = new List<int>();
        public string Delimiter { get; set; } = ",";
        public bool HasHeader { get; set; } = true;
        public bool Verbose { get; set; }
        public int Limit { get; set; } = -1;
        public bool ShowStats { get; set; }
        public bool PrintData { get; set; }
        public long RowLimit { get; set; } = -1;
        public bool FindDuplicates { get; set; }
    }

    // Interface for data providers
    public interface IDataProvider
    {
        Task<List<string>> GetFieldsToCheckAsync(List<string> fields, List<int> columnIndices);
        Task<DataTable> ReadDataAsync(List<string> fieldsToCheck);
        Task DisplayStatisticsAsync();
    }

    // Factory to create appropriate data provider
    public static class DataProviderFactory
    {
        public static IDataProvider CreateProvider(Options options)
        {
            string extension = Path.GetExtension(options.FilePath).ToLowerInvariant();

            // For directory paths, check if it contains parquet files
            if (Directory.Exists(options.FilePath) && Directory.GetFiles(options.FilePath, "*.parquet", SearchOption.AllDirectories).Any())
            {
                return new ParquetDataProvider(options);
            }

            // Based on file extension
            return extension switch
            {
                ".csv" => new CsvDataProvider(options),
                ".parquet" => new ParquetDataProvider(options),
                _ => throw new NotSupportedException($"File type not supported: {extension}")
            };
        }
    }

    // CSV data provider implementation
    public class CsvDataProvider : IDataProvider
    {
        private readonly Options _options;
        private List<dynamic> _cachedRecords;

        public CsvDataProvider(Options options)
        {
            _options = options;
        }

        private List<dynamic> ReadCsvData()
        {
            if (_cachedRecords != null)
                return _cachedRecords;

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = _options.Delimiter,
                HasHeaderRecord = _options.HasHeader,
                MissingFieldFound = null,
                BadDataFound = context => { /* Handle bad data */ }
            };

            using var reader = new StreamReader(_options.FilePath);
            using var csv = new CsvReader(reader, config);

            _cachedRecords = csv.GetRecords<dynamic>().ToList();
            return _cachedRecords;
        }

        public Task<List<string>> GetFieldsToCheckAsync(List<string> fields, List<int> columnIndices)
        {
            var records = ReadCsvData();
            if (records.Count == 0)
                return Task.FromResult(new List<string>());

            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();

            if (fields.Count == 0 && columnIndices.Count == 0)
            {
                Console.WriteLine("No fields or columns specified, using all available fields.");
                return Task.FromResult(allColumns);
            }

            if (columnIndices.Count > 0)
            {
                var fieldsFromIndices = columnIndices
                    .Where(i => i >= 0 && i < allColumns.Count)
                    .Select(i => allColumns[i])
                    .ToList();

                return Task.FromResult(fieldsFromIndices);
            }

            var validFields = fields.Where(f => allColumns.Contains(f, StringComparer.OrdinalIgnoreCase)).ToList();
            var invalidFields = fields.Except(validFields, StringComparer.OrdinalIgnoreCase).ToList();

            if (invalidFields.Any())
            {
                Console.WriteLine($"Warning: The following fields do not exist: {string.Join(", ", invalidFields)}");
            }

            return Task.FromResult(validFields);
        }

        public Task<DataTable> ReadDataAsync(List<string> fieldsToCheck)
        {
            var records = ReadCsvData();
            var dataTable = new DataTable();

            // Create columns
            foreach (var field in fieldsToCheck)
            {
                dataTable.Columns.Add(field);
            }

            // Add rows
            foreach (var record in records)
            {
                var row = dataTable.NewRow();
                var recordDict = (IDictionary<string, object>)record;

                foreach (var field in fieldsToCheck)
                {
                    row[field] = recordDict[field] ?? DBNull.Value;
                }

                dataTable.Rows.Add(row);
            }

            return Task.FromResult(dataTable);
        }

        public Task DisplayStatisticsAsync()
        {
            var records = ReadCsvData();
            if (records.Count == 0)
            {
                Console.WriteLine("No data to generate statistics.");
                return Task.CompletedTask;
            }

            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();

            Console.WriteLine("\nCSV File Statistics:");
            Console.WriteLine("-------------------");
            Console.WriteLine($"File Path: {_options.FilePath}");
            Console.WriteLine($"Total Rows: {records.Count}");
            Console.WriteLine($"Total Columns: {allColumns.Count}");

            Console.WriteLine("\nColumn Names:");
            Console.WriteLine("Index | Column Name");
            Console.WriteLine("------|------------");
            for (int i = 0; i < allColumns.Count; i++)
            {
                Console.WriteLine($"{i,5} | {allColumns[i]}");
            }

            return Task.CompletedTask;
        }
    }

    // Parquet data provider implementation
    public class ParquetDataProvider : IDataProvider
    {
        private readonly Options _options;
        private ParquetEngine _parquetEngine;
        private DataTable _cachedData;

        public ParquetDataProvider(Options options)
        {
            _options = options;
        }

        private async Task<ParquetEngine> GetParquetEngineAsync()
        {
            if (_parquetEngine == null)
            {
                Console.WriteLine($"Opening {_options.FilePath}...");
                _parquetEngine = await ParquetEngine.OpenFileOrFolderAsync(_options.FilePath, CancellationToken.None);
            }
            return _parquetEngine;
        }

        public async Task<List<string>> GetFieldsToCheckAsync(List<string> fields, List<int> columnIndices)
        {
            var parquetEngine = await GetParquetEngineAsync();
            var availableFields = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();

            if (fields.Count == 0 && columnIndices.Count == 0)
            {
                Console.WriteLine("No fields or columns specified, using all available fields.");
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
                Console.WriteLine($"Warning: The following fields do not exist: {string.Join(", ", invalidFields)}");
            }

            return validFields;
        }

        public async Task<DataTable> ReadDataAsync(List<string> fieldsToCheck)
        {
            if (_cachedData != null)
                return _cachedData;

            var parquetEngine = await GetParquetEngineAsync();
            Console.WriteLine($"Reading data with fields: {string.Join(", ", fieldsToCheck)}");

            var loadResult = await parquetEngine.ReadRowsAsync(
                fieldsToCheck,
                0,
                (int)parquetEngine.RecordCount,
                CancellationToken.None,
                null);

            _cachedData = loadResult.Invoke(false);
            return _cachedData;
        }

        public async Task DisplayStatisticsAsync()
        {
            var parquetEngine = await GetParquetEngineAsync();

            Console.WriteLine("\n===== PARQUET FILE STATISTICS =====");
            Console.WriteLine($"Type: {(Directory.Exists(_options.FilePath) ? "Folder" : "File")}");
            Console.WriteLine($"Path: {_options.FilePath}");
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");

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

            if (File.Exists(_options.FilePath))
            {
                var fileInfo = new FileInfo(_options.FilePath);
                Console.WriteLine($"\nFile Size: {FormatFileSize(fileInfo.Length)}");
            }
            else if (Directory.Exists(_options.FilePath))
            {
                var dirInfo = new DirectoryInfo(_options.FilePath);
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

    // Common data display utility
    public static class DataDisplayer
    {
        public static void DisplayData(DataTable dataTable)
        {
            // Print header
            foreach (DataColumn column in dataTable.Columns)
            {
                Console.Write($"{column.ColumnName,-36} | ");
            }
            Console.WriteLine();

            // Print separator
            Console.WriteLine(new string('-', dataTable.Columns.Count * 39));

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
            // Print header
            foreach (DataColumn column in dataTable.Columns)
            {
                Console.Write($"{column.ColumnName,-36} | ");
            }
            Console.WriteLine();

            // Print separator
            Console.WriteLine(new string('-', dataTable.Columns.Count * 39));

            // Print rows up to limit
            int rowCount = 0;
            foreach (DataRow row in dataTable.Rows)
            {
                if (rowCount >= limit) break;

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
                rowCount++;
            }
        }
    }

    // Unified duplicate finder
    public static class DuplicateFinder
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
                    Console.WriteLine();

                    int recordNumber = 1;
                    foreach (var row in group.Value)
                    {
                        Console.Write($"{recordNumber++,5}:");
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            var value = row[column];
                            var displayValue = value?.ToString() ?? "NULL";
                            if (displayValue.Length > 36)
                                displayValue = displayValue.Substring(0, 36);

                            Console.Write($"{displayValue,-36} | ");
                        }
                        Console.WriteLine();
                    }
                }
                else
                {
                    // Header for selected fields
                    foreach (var field in fieldsToCheck)
                    {
                        Console.Write($"{field,-36} | ");
                    }
                    Console.WriteLine();

                    // Display first record values for selected fields
                    foreach (var field in fieldsToCheck)
                    {
                        var value = group.Value[0][field]?.ToString() ?? "NULL";
                        if (value.Length > 36)
                            value = value.Substring(0, 36);

                        Console.Write($"{value,-36} | ");
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

            // Compute the SHA256 hash of the concatenated string for better performance with large data
            using (var sha256 = SHA256.Create())
            {
                var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(keyBuilder.ToString()));
                return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
            }
        }
    }
}
