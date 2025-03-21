using CsvHelper.Configuration;
using CsvHelper;
using ParquetViewer.Engine;
using ParquetViewer.Engine.Exceptions;
using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

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

                if (options.PrimaryKeyColumns != null && options.PrimaryKeyColumns.ContainsKey(Path.GetFileName(options.FilePath)))
                {
                    var pkColumns = options.PrimaryKeyColumns[Path.GetFileName(options.FilePath)];
                    Console.WriteLine($"Primary Key Columns for {options.FilePath}: {string.Join(", ", pkColumns)}");
                }
                else
                {
                    Console.WriteLine("No primary key columns loaded from config.");
                }

                if (options != null)
                {
                    if(options.Verbose || options.ShowStats)CommandLineParser.PrintCurrentConfiguration(options);
                }

                DataTable dataTable;

                if (options.IsCsv)
                {
                    dataTable = CsvOperations.ReadCsv(options);
                }
                else
                {
                    var parquetEngine = await ParquetOperations.OpenFileAsync(options);
                    var fieldsToCheck = ParquetOperations.GetFieldsToCheck(parquetEngine, options);
                    dataTable = await ParquetOperations.ReadDataAsync(parquetEngine, fieldsToCheck);
                }

                if (options.ShowStats)
                {
                    PrintData.DisplayColumnsData(dataTable, options.ColumnIndices);
                    return;
                }

                if (options.PrintData)
                {
                    PrintData.DisplayData(dataTable, options.RowLimit);
                }

                if (options.FindDuplicates)
                {
                    if (options.Fields.Count == 0)
                    {
                        if(options.Verbose)  Console.WriteLine("No fields specified. Using all columns.");
                        options.Fields = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
                    }
                    if (options.Verbose) Console.WriteLine($"Fields used for duplicate check [{options.Fields.Count}]: {string.Join(", ", options.Fields)}");

                    DuplicateFinder.FindAndDisplayDuplicates(dataTable, options.Fields, options.Verbose, options.Limit);
                }
            }
            catch (Exception ex)
            {
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
    }

    // Command-line argument parsing
    static class CommandLineParser
    {
        private const string DefaultConfigFile = "pklist.json";
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
                "--csv",
                "--delimiter",
                "--header",
                "-f", "--fields",
                "-c", "--columns",
                "-v", "--verbose",
                "-l", "--limit",
                "-d", "--findDuplicates",
                "-pf", "--printData",
                "-s", "--stats",
                "-h", "--help"
                ,"--config"
            };

            bool configSpecified = false;

            for (int i = 1; i < args.Length; i++)
            {
                string arg = args[i];

                switch (arg)
                {
                    case "--config":
                        if (i + 1 < args.Length)
                        {
                            options.ConfigFilePath = args[i + 1];
                            configSpecified = true;
                            i++;
                        }                        
                        break;
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
                    case "-s":
                    case "--stats":
                        options.ShowStats = true;
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

            // Default to pklist.json in current directory if not specified
            if (!configSpecified && File.Exists(DefaultConfigFile))
            {
                options.ConfigFilePath = DefaultConfigFile;
            }

            // Load PK columns from config only if neither -f nor -c is specified
            if (options.Fields == null && options.ColumnIndices == null && !string.IsNullOrEmpty(options.ConfigFilePath))
            {
                options.PrimaryKeyColumns = LoadPrimaryKeyColumns(options.ConfigFilePath, options.FilePath);
            }

            // Warnings for conflicts
            if (options.Fields != null && (options.ConfigFilePath != null || options.ColumnIndices != null))
            {
                Console.WriteLine("Warning: '--fields' specified; ignoring '--config' and '--columns' for column selection.");
            }
            else if (options.ColumnIndices != null && options.ConfigFilePath != null)
            {
                Console.WriteLine("Warning: '--columns' specified; ignoring '--config' for column selection.");
            }

            return options;
        }

        public static void PrintCurrentConfiguration(Options options)
        {
            Console.WriteLine("══════ Command Line Configuration ══════");
            Console.WriteLine($"File: {options.FilePath}");
            Console.WriteLine($"Mode: {(options.IsCsv ? "CSV" : "Parquet")}");

            if (options.IsCsv)
            {
                Console.WriteLine($"Delimiter: '{options.Delimiter}' | Header: {options.HasHeader}");
            }

            Console.WriteLine($"Fields: {(options.Fields.Count > 0 ? string.Join(", ", options.Fields) : "All Columns")}");
            Console.WriteLine($"Columns: {(options.ColumnIndices.Count > 0 ? string.Join(", ", options.ColumnIndices) : "N/A")}");
            Console.WriteLine($"Verbose: {options.Verbose} | Duplicates: {options.FindDuplicates} | Limit: {(options.Limit > 0 ? options.Limit : "Unlimited")}");
            Console.WriteLine($"Print Data: {options.PrintData} | Row Limit: {(options.RowLimit > 0 ? options.RowLimit : "All")}");

            // If Parquet file, show additional statistics
            if (!options.IsCsv)
            {
                var parquetEngine = ParquetOperations.OpenFileAsync(options).Result;
                Console.WriteLine("════════ Parquet File Statistics ═══════");
                Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
                Console.WriteLine($"Number of Columns: {parquetEngine.Schema.Fields.Count()}");

                // Schema Type Summary
                var schemaTypeCounts = parquetEngine.Schema.DataFields
                    .GroupBy(f => f.SchemaType)
                    .Select(g => new { Type = g.Key, Count = g.Count() })
                    .OrderByDescending(x => x.Count);

                Console.WriteLine("Schema Type Summary:");
                foreach (var typeCount in schemaTypeCounts)
                {
                    Console.WriteLine($"  {typeCount.Type,-20}: {typeCount.Count} column(s)");
                }

                // File Size
                if (File.Exists(options.FilePath))
                {
                    var fileInfo = new FileInfo(options.FilePath);
                    Console.WriteLine($"\nFile Size: {ParquetStatistics.FormatFileSize(fileInfo.Length)}");
                }
            }
            Console.WriteLine("════════════════════════════════════════");
        }

        private static Dictionary<string, List<string>> LoadPrimaryKeyColumns(string configFilePath, string targetFilePath)
        {
            try
            {
                string jsonContent = File.ReadAllText(configFilePath);

                // Use the source-generated context
                var config = JsonSerializer.Deserialize(jsonContent, ConfigJsonContext.Default.ConfigFile);

                var pkColumns = new Dictionary<string, List<string>>();
                string fileName = Path.GetFileName(targetFilePath);

                if (config.ParquetFiles.ContainsKey(fileName))
                {
                    pkColumns[fileName] = config.ParquetFiles[fileName].Columns
                        .Where(c => c.IsPrimaryKey)
                        .Select(c => c.Name)
                        .ToList();
                }
                else if (config.ParquetFiles.ContainsKey("*.parquet"))
                {
                    pkColumns[fileName] = config.ParquetFiles["*.parquet"].Columns
                        .Where(c => c.IsPrimaryKey)
                        .Select(c => c.Name)
                        .ToList();
                }
                else
                {
                    Console.WriteLine($"No config entry found for '{fileName}'.");
                }

                return pkColumns;
            }
            catch (FileNotFoundException)
            {
                Console.WriteLine($"Config file not found: {configFilePath}");
                return null;
            }
            catch (JsonException ex)
            {
                Console.WriteLine($"Error parsing config file '{configFilePath}': {ex.Message}");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Unexpected error loading config file '{configFilePath}': {ex.Message}");
                return null;
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine("ParquetDuplicateFinder - Find duplicate records in Parquet or CSV files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  ParquetDuplicateFinder <file_or_folder_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  --config <path>                   Path to JSON config file with PK columns");
            Console.WriteLine("  --csv                             Process a CSV file instead of Parquet");
            Console.WriteLine("  --delimiter <char>                Specify CSV delimiter (default: ',')");
            Console.WriteLine("  --header                          Treat first row as CSV header");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Specify fields to check for duplicates");
            Console.WriteLine("  -c, --columns <index1,index2,...> Specify column indices for duplicates");
            Console.WriteLine("  -v, --verbose                     Show all fields for duplicate records");
            Console.WriteLine("  -l, --limit <number>              Limit number of duplicate groups to display");
            Console.WriteLine("  -d, --findDuplicates              Find and Display Duplicates");
            Console.WriteLine("  -pf, --printData                  Display Parquet/CSV Data");
            Console.WriteLine("  -s, --stats                       Display Parquet/CSV Column Data");
            Console.WriteLine("  -h, --help                        Show this help message");
        }
    }

    // Options class to hold command-line arguments
    public class Options
    {
        public string FilePath { get; set; }
        public List<string> Fields { get; set; } = new List<string>();
        public List<int> ColumnIndices { get; set; } = new List<int>();
        public bool Verbose { get; set; }
        public int Limit { get; set; } = -1;
        public bool FindDuplicates { get; set; }
        public bool PrintData { get; set; }
        public long RowLimit { get; set; } = -1;
        public bool IsCsv { get; set; }
        public char Delimiter { get; set; } = ',';
        public bool HasHeader { get; set; } = true;
        public bool ShowStats { get; set; }
        public string ConfigFilePath { get; set; }
        public Dictionary<string, List<string>> PrimaryKeyColumns { get; set; } // Maps file name to its PK columns
    }


    static class CsvOperations
    {
        public static DataTable ReadCsv(Options options)
        {
            string filePath = options.FilePath;
            char delimiter = options.Delimiter;
            bool hasHeader = options.HasHeader;
            List<string> selectedFields = options.Fields;
            List<int> selectedIndices = options.ColumnIndices;
                        
            if(options.Verbose) Console.WriteLine($"Using delimiter: '{delimiter}'");

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

            if (!csv.Read()) return dataTable; // No data

            // Determine which columns to load
            List<int> columnIndicesToLoad = new();

            // Read headers
            if (hasHeader)
            {
                csv.ReadHeader();
                var headers = csv.HeaderRecord;


                if (selectedFields.Count > 0)
                {
                    // Get column indices from field names
                    columnIndicesToLoad = selectedFields
                        .Select(f => Array.IndexOf(headers, f))
                        .Where(index => index >= 0)
                        .ToList();
                }
                else if (selectedIndices.Count > 0)
                {
                    // Use selected column indices
                    columnIndicesToLoad = selectedIndices.Where(i => i >= 0 && i < headers.Length).ToList();
                }
                else
                {
                    // Load all columns if no filters are specified
                    columnIndicesToLoad = Enumerable.Range(0, headers.Length).ToList();
                }

                // Add selected columns to DataTable
                foreach (int index in columnIndicesToLoad)
                {
                    dataTable.Columns.Add(headers[index]);
                }

                if (options.Verbose) Console.WriteLine($"Selected CSV Columns: {string.Join(", ", columnIndicesToLoad.Select(i => headers[i]))}");
            }

            // Read rows
            while (csv.Read())
            {
                var row = dataTable.NewRow();
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    row[i] = csv.GetField(columnIndicesToLoad[i])?.Trim() ?? "NULL";
                }
                dataTable.Rows.Add(row);
            }

            Console.WriteLine($"Total rows loaded: {dataTable.Rows.Count}");
            return dataTable;
        }
    }

    // Parquet file operations
    static class ParquetOperations
    {
        public static async Task<ParquetEngine> OpenFileAsync(Options options)
        {

            if(options.Verbose)Console.WriteLine($"Opening {options.FilePath}...");
            return await ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None);
        }

        public static List<string> GetFieldsToCheck(ParquetEngine parquetEngine, Options options)
        {
            List<string> fields = options.Fields;
            List< int > columnIndices = options.ColumnIndices;
            var availableFields = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();

            if (fields.Count == 0 && columnIndices.Count == 0)
            {
                if(options.Verbose) Console.WriteLine("No fields or columns specified, using all available fields for duplicate checking.");
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
            Console.WriteLine($"Searching for duplicates...[{dataTable.Rows.Count}]");

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

            int groupNumber = 1;
            foreach (var group in duplicateGroupsFiltered)
            {
                Console.WriteLine($"\nDuplicate Group #{groupNumber++} - {group.Value.Count} records");

                if (verbose)
                {
                    Console.WriteLine($"{"#####",5} | " + string.Join(" | ", dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));

                    int recordNumber = 1;
                    foreach (var row in group.Value)
                    {
                        Console.Write($"{recordNumber++,5} | ");
                        Console.WriteLine(string.Join(" | ", dataTable.Columns.Cast<DataColumn>().Select(c => (row[c]?.ToString() ?? "NULL")[..Math.Min(36, (row[c]?.ToString() ?? "").Length)])));
                    }
                }
                else
                {
                    Console.WriteLine(string.Join(" | ", fieldsToCheck));
                    Console.WriteLine(string.Join(" | ", fieldsToCheck.Select(f => (group.Value[0][f]?.ToString() ?? "NULL")[..Math.Min(36, (group.Value[0][f]?.ToString() ?? "").Length)])));
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
        public static void DisplayColumnsData(DataTable dataTable, List<int> selectedColumnIndices)
        {
            Console.WriteLine("═════ COLUMN DETAILS ═════");

            // Print all columns in the DataTable
            Console.WriteLine("All Available Columns:");
            for (int i = 0; i < dataTable.Columns.Count; i++)
            {
                Console.WriteLine($"  {i, 2}. {dataTable.Columns[i].ColumnName}");
            }

            Console.WriteLine("═════════════════════════");
        }

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
            Console.WriteLine("");
            Console.WriteLine(new string('─', dataTable.Columns.Count*(36+3)-1));

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
            Console.WriteLine("═════ PARQUET FILE STATISTICS ═════");
            Console.WriteLine($"Type: {(Directory.Exists(filePath) ? "Folder" : "File")}");
            Console.WriteLine($"Path: {filePath}");
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
            //Console.WriteLine($"Number of Partitions: {parquetEngine.NumberOfPartitions}");
            //Console.WriteLine($"Number of Row Groups: {parquetEngine.ThriftMetadata.RowGroups.Count}");

            var dataFields = parquetEngine.Schema.DataFields;
            Console.WriteLine($"Columns: {dataFields.Length}");
            Console.WriteLine("Column Details:");
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

            Console.WriteLine("Schema Type Summary:");
            foreach (var typeCount in schemaTypeCounts)
            {
                Console.WriteLine($"  {typeCount.Type,-20}: {typeCount.Count} column(s)");
            }

            if (File.Exists(filePath))
            {
                var fileInfo = new FileInfo(filePath);
                Console.WriteLine($"File Size: {FormatFileSize(fileInfo.Length)}");
            }
            else if (Directory.Exists(filePath))
            {
                var dirInfo = new DirectoryInfo(filePath);
                var files = dirInfo.GetFiles("*.parquet", SearchOption.AllDirectories);
                long totalSize = files.Sum(f => f.Length);
                Console.WriteLine($"Total Size of Parquet Files: {FormatFileSize(totalSize)}");
                Console.WriteLine($"Number of Parquet Files: {files.Length}");
            }

            Console.WriteLine("════════════════════════");
        }

        public static string FormatFileSize(long bytes)
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


    public class ColumnConfig
    {
        public string Name { get; set; }
        public bool IsPrimaryKey { get; set; }
    }

    public class ParquetFileConfig
    {
        public List<ColumnConfig> Columns { get; set; }
    }

    public class ConfigFile
    {
        public Dictionary<string, ParquetFileConfig> ParquetFiles { get; set; }
    }
}