using CsvHelper.Configuration;
using CsvHelper;
using ParquetViewer.Engine.Exceptions;
using System.Data;
using System.Globalization;
using System.Text.Json.Serialization;
using System.Text.Json;
using ParquetViewer.Engine;
using System.Text;
using System.Security.Cryptography;
using System.IO.Compression;

namespace ParquetDuplicateFinder;

partial class Program
{
    private static string logFilePath;
    static async Task Main(string[] args)
    {
        try
        {
            InitializeLogging(new());
            var options = CommandLineParser.Parse(args);
            if (options == null) return;
            
            var filesToProcess = options.IsFolderMode
                ? Directory.EnumerateFiles(options.FolderPath, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase))
                : new[] { options.FilePath };

            foreach (var filePath in filesToProcess)
            {
                Log($"Processing file: {filePath}");

                if (!Program.IsFileAvailable(filePath))
                    continue;

                UpdateOptionsForFile(options, filePath);

                if (options.ReconfigColumns)
                {
                    ConfigManager.ReconfigColumnsFromFile(options);
                    Log("Updated pklist.json with file columns.");
                    continue;
                }

                // Determine columns to use based on updated options
                List<string> columnsToUse = await GetColumnsToUse(options);

                if (options.Verbose || options.ShowStats)
                {
                    FileInfoProvider.DisplayFileInfo(options);
                    LogFileInfo(options);
                }

                DataTable dataTable = options.IsCsv
                    ? CsvOperations.LoadCsv(options, columnsToUse)
                    : await ParquetOperations.LoadParquet(options, columnsToUse);

                if (options.ShowStats)
                {
                    LogColumnStats(dataTable, options);
                }

                if (options.PrintData)
                {
                    LogData(dataTable, options);
                }

                if (options.FindDuplicates)
                {
                    LogDuplicates(dataTable, columnsToUse, options);
                }

                Log($"Finished processing file: {filePath}");
            }
        }
        catch (Exception ex)
        {
            Log($"Error: {ex.Message}");
            if (ex is AllFilesSkippedException afse) Log($"Details: {afse.Message}");
            else if (ex is SomeFilesSkippedException sfse) Log($"Details: {sfse.Message}");
            else if (ex is FileReadException fre) Log($"Details: {fre.Message}");
            else if (ex is MultipleSchemasFoundException msfe) Log($"Details: {msfe.Message}");
        }
    }    
}

partial class Program
{
    public static bool IsFileAvailable(string filePath)
    {
        try
        {
            using (FileStream fs = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                return true;
            }
        }
        catch (IOException)        
        {
            Log($"{filePath} unable to access file.");
            return false;        
        }
        catch (UnauthorizedAccessException)        
        {
            Log($"{filePath} unable to access file.");
            return false;        
        }
    }
    private static async Task<List<string>> GetColumnsToUse(Options options)
    {
        if (options.IsCsv)
        {
            using var fileStream = new FileStream(options.FilePath, FileMode.Open, FileAccess.Read);
            Stream stream = fileStream;

            if (options.FilePath.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase))
            {
                stream = new GZipStream(fileStream, CompressionMode.Decompress);
            }

            using var reader = new StreamReader(stream);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = options.Delimiter.ToString(), HasHeaderRecord = options.HasHeader });
            if (csv.Read() && options.HasHeader)
            {
                csv.ReadHeader();
                return ColumnSelector.GetColumnsToUse(options, csv.HeaderRecord);
            }
            return ColumnSelector.GetColumnsToUse(options);
        }
        else
        {
            var parquetEngine = await ParquetOperations.LoadParquet(options, new List<string> { });
            var availableFields = parquetEngine.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
            return ColumnSelector.GetColumnsToUse(options, null, availableFields);
        }
    }

    private static void InitializeLogging(Options options)
    {
        Directory.CreateDirectory(options.LogFolder);
        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        logFilePath = Path.Combine(options.LogFolder, $"Run_{timestamp}.log");
        Log("Starting ParquetDuplicateFinder run...");
    }

    public static void Log(string message)
    {
        //Console.WriteLine(message);
        File.AppendAllText(logFilePath, $"{DateTime.Now}: {message}\n");
    }
    public static void LogLineBreak() => File.AppendAllText(logFilePath, Environment.NewLine);
    

    private static void LogFileInfo(Options options)
    {
        Log("══════ File Information ══════");
        Log($"File: {options.FilePath}");
        Log($"Mode: {(options.IsCsv ? "CSV" : "Parquet")}");

        var fileInfo = new FileInfo(options.FilePath);
        if (options.IsCsv)
        {
            Log($"Delimiter: '{options.Delimiter}' | Header: {options.HasHeader} | File Size: {FileInfoProvider.FormatFileSize(fileInfo.Length)}");
        }
        else
        {
            var parquetEngine = ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None).Result;
            Log($"Total Records: {parquetEngine.RecordCount:N0}");
            Log($"Number of Columns: {parquetEngine.Schema.Fields.Count()}");
            Log("Schema Type Summary:");
            var schemaTypes = parquetEngine.Schema.DataFields
                .GroupBy(f => f.SchemaType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);
            foreach (var type in schemaTypes)
            {
                Log($"  {type.Type,-20}: {type.Count} column(s)");
            }
            Log($"File Size: {FileInfoProvider.FormatFileSize(fileInfo.Length)}");
        }
        Log("══════════════════════════════");
    }

    private static void LogColumnStats(DataTable dataTable, Options options)
    {
        if (options.Verbose) Log("Displaying column statistics...");
        Log("═════ Column Details ═════");
        Log("All Available Columns:");
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            Log($"  {i,2}. {dataTable.Columns[i].ColumnName}");
        }
        Log("═════════════════════════");
    }

    private static void LogData(DataTable dataTable, Options options)
    {
        if (options.Verbose) Log("Printing data...");

        const int MAX_COLUMN_WIDTH = 40;
        const int SAMPLE_SIZE = 100;

        int[] columnWidths = new int[dataTable.Columns.Count];
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            string header = dataTable.Columns[i].ColumnName;
            int sampleWidth = dataTable.Rows.Cast<DataRow>()
                .Take(Math.Min(SAMPLE_SIZE, dataTable.Rows.Count))
                .Select(r => (r[i]?.ToString() ?? "NULL").Length)
                .DefaultIfEmpty(0)
                .Max();
            columnWidths[i] = Math.Min(Math.Max(header.Length, sampleWidth), MAX_COLUMN_WIDTH);
        }

        Log(string.Join(" | ", dataTable.Columns.Cast<DataColumn>()
            .Select((c, i) => c.ColumnName.PadRight(columnWidths[i]))));
        Log(new string('─', columnWidths.Sum() + (dataTable.Columns.Count - 1) * 3));

        long rowCount = 0;
        foreach (DataRow row in dataTable.Rows)
        {
            if (options.RowLimit == -1 || rowCount < options.RowLimit)
            {
                Log(string.Join(" | ", dataTable.Columns.Cast<DataColumn>()
                    .Select((c, i) =>
                    {
                        string value = row[c]?.ToString() ?? "NULL";
                        return value.Length > columnWidths[i]
                            ? value[..columnWidths[i]]
                            : value.PadRight(columnWidths[i]);
                    })));
                rowCount++;
            }
            else break;
        }

        if (options.Verbose) Log($"Printed {rowCount} rows.");
        LogLineBreak();
    }

    private static void LogDuplicates(DataTable dataTable, List<string> columnsToUse, Options options)
    {
        if (options.Verbose) Log($"Finding duplicates using columns: {string.Join(", ", columnsToUse)}");

        var duplicateGroups = new Dictionary<string, List<(DataRow Row, int Position)>>();
        int rowPosition = 0;
        Parallel.ForEach(dataTable.AsEnumerable(), row =>
        {
            var key = CreateKey(row, columnsToUse);
            lock (duplicateGroups)
            {
                if (!duplicateGroups.ContainsKey(key))
                    duplicateGroups[key] = new List<(DataRow, int)>();
                duplicateGroups[key].Add((row, rowPosition));
            }
            Interlocked.Increment(ref rowPosition);
        });

        var duplicates = duplicateGroups
            .Where(g => g.Value.Count > 1)
            .OrderByDescending(g => g.Value.Count)
            .ToList();

        if (duplicates.Count == 0)
        {
            Log($"[{options.FilePath}] : No duplicates found.");
            return;
        }

        Log($"Found {duplicates.Count} duplicate groups.");
        Log("═════ Duplicate Summary ═════");
        Log($"{"Group #",-8} | {"Count",-6} | {"Row #",-12} | Record");
        Log(new string('─', 8 + 3 + 6 + 3 + 12 + 3 + columnsToUse.Count * 20));

        int displayLimit = options.Limit > 0 ? Math.Min(options.Limit, duplicates.Count) : duplicates.Count;
        for (int i = 0; i < displayLimit; i++)
        {
            var group = duplicates[i];
            var sample = group.Value[0];
            var sampleValues = string.Join(" | ", columnsToUse
                .Select(c => (sample.Row[c]?.ToString() ?? "NULL").PadRight(20)[..Math.Min(20, (sample.Row[c]?.ToString() ?? "").Length)]));
            Log($"{i + 1,-8} | {group.Value.Count,-6} | {sample.Position,-12} | {sampleValues}");
        }

        int totalDuplicates = duplicates.Sum(g => g.Value.Count - 1);
        Log($"\nSummary: Found {totalDuplicates} duplicate records in {duplicates.Count} groups.");
    }

    public static string CreateKey(DataRow row, List<string> columnsToUse)
    {
        var keyBuilder = new StringBuilder();
        foreach (var column in columnsToUse)
        {
            keyBuilder.Append(row[column]?.ToString() ?? "NULL").Append("|");
        }
        using var sha256 = SHA256.Create();
        var hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(keyBuilder.ToString()));
        return BitConverter.ToString(hashBytes).Replace("-", "").ToLowerInvariant();
    }

    public static void UpdateOptionsForFile(Options options, string filePath)
    {
        options.FilePath = filePath;

        if (filePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
            filePath.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase))
        {
            options.IsCsv = true;
            options.Delimiter = options.Delimiter != '\0' ? options.Delimiter : ',';
            options.HasHeader = options.HasHeader;
        }
        else if (filePath.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                 filePath.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase))
        {
            options.IsCsv = false;
        }
        else
        {
            Program.Log($"Skipping unsupported file: {filePath}");
            return;
        }

        // Load config only for actual files being processed
        if (options.Fields == null && options.ColumnIndices == null && !string.IsNullOrEmpty(options.ConfigFilePath))
            options.PrimaryKeyColumns = LoadPrimaryKeyColumns(options.ConfigFilePath, filePath);
    }

    public static Dictionary<string, List<string>> LoadPrimaryKeyColumns(string configFilePath, string targetFilePath)
    {
        try
        {
            string jsonContent = File.ReadAllText(configFilePath);
            var config = JsonSerializer.Deserialize(jsonContent, ConfigJsonContext.Default.ConfigFile);

            if (config == null || config.ParquetFiles == null)
            {
                Program.Log($"Config file '{configFilePath}' is empty or missing 'ParquetFiles'.");
                return null;
            }

            var pkColumns = new Dictionary<string, List<string>>();
            string fileName = Path.GetFileName(targetFilePath);

            if (config.ParquetFiles.ContainsKey(fileName))
            {
                var columns = config.ParquetFiles[fileName].Columns;
                if (columns != null)
                {
                    pkColumns[fileName] = columns
                        .Where(c => c.IsPrimaryKey)
                        .Select(c => c.Name)
                        .ToList();
                }
                else
                {
                    Program.Log($"No columns defined for '{fileName}' in config file '{configFilePath}'.");
                    return null;
                }
            }
            else
            {
                Program.Log($"No entry found for '{fileName}' in config file '{configFilePath}'.");
                return null;
            }

            return pkColumns.Count > 0 ? pkColumns : null;
        }
        catch (Exception ex)
        {
            Program.Log($"Error loading config file '{configFilePath}': {ex.Message}");
            return null;
        }
    }
}
static class CommandLineParser
{
    private const string DefaultConfigFile = "pklist.json";
    private static readonly HashSet<string> ValidArgs = new HashSet<string>
    {
        "--folder",
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
        ,"--reconfigColumns"
    };

    public static Options Parse(string[] args)
    {
        if (args.Length < 1)
        {
            UserFeedback.PrintUsage();
            return null;
        }

        var options = ParseArguments(args);
        if (options == null) return null;

        if (options.ReconfigColumns) return options;

        if (!ValidateOptions(options))
            return null;

        WarnAboutConflicts(options);

        return options;
    }

    private static Options ParseArguments(string[] args)
    {
        var initialPath = args[0].Equals(".") ? Directory.GetCurrentDirectory() : args[0];
        var options = new Options();

        // Check if initial path is a directory and set mode accordingly
        if (Directory.Exists(initialPath))
        {
            options.IsFolderMode = true;
            options.FolderPath = initialPath;
        }
        else
        {
            options.FilePath = initialPath;
        }

        bool configSpecified = false;

        for (int i = 1; i < args.Length; i++)
        {
            string arg = args[i];
            if (!ValidArgs.Contains(arg))
            {
                UserFeedback.PrintInvalidArgumentError(arg);
                return null;
            }

            switch (arg)
            {
                case "--folder":
                    options.IsFolderMode = true;
                    options.FolderPath = options.FilePath ?? initialPath;
                    break;
                case "--config":
                    if (i + 1 < args.Length)
                    {
                        options.ConfigFilePath = args[++i];
                        configSpecified = true;
                    }
                    break;
                case "--csv":
                    options.IsCsv = true;
                    break;
                case "--delimiter":
                    if (i + 1 < args.Length) options.Delimiter = args[++i][0];
                    break;
                case "--header":
                    options.HasHeader = true;
                    break;
                case "-f":
                case "--fields":
                    if (i + 1 < args.Length) options.Fields = args[++i].Split(',').Select(f => f.Trim()).ToList();
                    break;
                case "-c":
                case "--columns":
                    if (i + 1 < args.Length)
                        options.ColumnIndices = args[++i].Split(',')
                            .Where(n => int.TryParse(n, out _))
                            .Select(int.Parse)
                            .ToList();
                    break;
                case "-v":
                case "--verbose":
                    options.Verbose = true;
                    break;
                case "-l":
                case "--limit":
                    if (i + 1 < args.Length && int.TryParse(args[++i], out int limit)) options.Limit = limit;
                    break;
                case "-d":
                case "--findDuplicates":
                    options.FindDuplicates = true;
                    break;
                case "-pf":
                case "--printData":
                    options.PrintData = true;
                    if (i + 1 < args.Length && long.TryParse(args[++i], out long rowLimit)) options.RowLimit = rowLimit;
                    else options.RowLimit = -1;
                    break;
                case "-s":
                case "--stats":
                    options.ShowStats = true;
                    break;
                case "-h":
                case "--help":
                    UserFeedback.PrintUsage();
                    return null;
                case "--reconfigColumns":
                    options.ReconfigColumns = true;
                    break;
            }
        }

        if (!configSpecified && File.Exists(DefaultConfigFile))
            options.ConfigFilePath = DefaultConfigFile;

        return options;
    }

    private static bool ValidateOptions(Options options)
    {
        if (options.IsFolderMode)
        {
            if (!Directory.Exists(options.FolderPath))
            {
                UserFeedback.PrintError($"Folder '{options.FolderPath}' does not exist.");
                return false;
            }
        }
        else if (!File.Exists(options.FilePath))
        {
            UserFeedback.PrintError($"File '{options.FilePath}' does not exist.");
            return false;
        }
        return true;
    }

    private static void WarnAboutConflicts(Options options)
    {
        if (options.Fields != null && (options.ColumnIndices != null || options.ConfigFilePath != null))
            UserFeedback.PrintWarning("'--fields' specified; ignoring '--config' and '--columns'.");
        else if (options.ColumnIndices != null && options.ConfigFilePath != null)
            UserFeedback.PrintWarning("'--columns' specified; ignoring '--config'.");
    }
    
    private static class UserFeedback
    {
        public static void PrintUsage()
        {
            Console.WriteLine("ParquetDuplicateFinder - Find duplicates in Parquet or CSV files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  ParquetDuplicateFinder <file_or_folder_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  --folder                          Process all files in the specified folder");
            Console.WriteLine("  --config <path>                   Path to JSON config file with PK columns");
            Console.WriteLine("  --csv                             Process a CSV file instead of Parquet");
            Console.WriteLine("  --delimiter <char>                CSV delimiter (fixed to ',' for folder mode)");
            Console.WriteLine("  --header                          Treat first CSV row as header (always true for CSV)");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Fields to check for duplicates");
            Console.WriteLine("  -c, --columns <index1,index2,...> Column indices for duplicates");
            Console.WriteLine("  -v, --verbose                     Show detailed output");
            Console.WriteLine("  -l, --limit <number>              Limit duplicate groups displayed");
            Console.WriteLine("  -d, --findDuplicates              Find and display duplicates");
            Console.WriteLine("  -pf, --printData                  Print file data");
            Console.WriteLine("  -s, --stats                       Show column statistics");
            Console.WriteLine("  --reconfigColumns                 Update pklist.json with file columns");
            Console.WriteLine("  -h, --help                        Show this help message");
        }

        public static void PrintError(string message)
        {
            Console.WriteLine($"Error: {message}");
        }

        public static void PrintWarning(string message)
        {
            Console.WriteLine($"Warning: {message}");
        }

        public static void PrintInvalidArgumentError(string arg)
        {
            Console.WriteLine($"Invalid argument: {arg}");
            PrintUsage();
        }
    }
}

public class Options
{
    public string FilePath { get; set; }
    public string FolderPath { get; set; } // New: Folder path for folder mode
    public bool IsFolderMode { get; set; } // New: Flag for folder mode
    public string LogFolder { get; set; } = "Logs"; // New: Default log folder
    public List<string> Fields { get; set; }
    public List<int> ColumnIndices { get; set; }
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
    public Dictionary<string, List<string>> PrimaryKeyColumns { get; set; }
    public bool ReconfigColumns { get; set; }
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

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true, WriteIndented = true)]
[JsonSerializable(typeof(ConfigFile))]
[JsonSerializable(typeof(ParquetFileConfig))]
[JsonSerializable(typeof(ColumnConfig))]
[JsonSerializable(typeof(Dictionary<string, ParquetFileConfig>))]
[JsonSerializable(typeof(List<ColumnConfig>))]
public partial class ConfigJsonContext : JsonSerializerContext { }



static class ColumnSelector
{
    public static List<string> GetColumnsToUse(Options options, string[] csvHeaders = null, List<string> parquetFields = null)
    {
        string fileName = Path.GetFileName(options.FilePath);
        List<string> allColumns = GetAllColumns(options, csvHeaders, parquetFields);

        // Priority 1: --fields
        if (TryUseFields(options, out var columnsToUse)) return columnsToUse;

        // Priority 2: --columns
        if (TryUseColumnIndices(options, csvHeaders, parquetFields, out columnsToUse)) return columnsToUse;

        // Priority 3: --config with fallback to all columns
        if (TryUseConfig(options, fileName, allColumns, out columnsToUse)) return columnsToUse;

        // Priority 4: All columns
        if (allColumns != null)
        {
            if (options.Verbose)
            {
                string source = options.IsCsv ? "CSV columns" : "Parquet fields";
                Console.WriteLine($"No specific columns provided; using all {source}: {string.Join(", ", allColumns)}");
                Program.Log($"No specific columns provided; using all {source}: {string.Join(", ", allColumns)}");
            }
            return allColumns;
        }

        throw new InvalidOperationException("No columns specified and no schema available to default to all columns.");
    }

    // Helper method to get all available columns
    private static List<string> GetAllColumns(Options options, string[] csvHeaders, List<string> parquetFields)
    {
        if (options.IsCsv && csvHeaders != null) return csvHeaders.ToList();
        if (!options.IsCsv && parquetFields != null) return parquetFields;
        return null;
    }

    // Priority 1: Use --fields
    private static bool TryUseFields(Options options, out List<string> columnsToUse)
    {
        columnsToUse = null;
        if (options.Fields != null && options.Fields.Count > 0)
        {
            columnsToUse = options.Fields;
            if (options.Verbose) Console.WriteLine($"Using fields from --fields: {string.Join(", ", columnsToUse)}");
            return true;
        }
        return false;
    }

    // Priority 2: Use --columns
    private static bool TryUseColumnIndices(Options options, string[] csvHeaders, List<string> parquetFields, out List<string> columnsToUse)
    {
        columnsToUse = null;
        if (options.ColumnIndices == null || options.ColumnIndices.Count == 0) return false;

        if (options.IsCsv && csvHeaders != null)
        {
            columnsToUse = options.ColumnIndices
                .Where(i => i >= 0 && i < csvHeaders.Length)
                .Select(i => csvHeaders[i])
                .ToList();
            if (columnsToUse.Count == 0) throw new ArgumentException("No valid column indices match the CSV headers.");
            if (options.Verbose) Console.WriteLine($"Mapped column indices from --columns (CSV): {string.Join(", ", columnsToUse)}");
            return true;
        }

        if (!options.IsCsv && parquetFields != null)
        {
            columnsToUse = options.ColumnIndices
                .Where(i => i >= 0 && i < parquetFields.Count)
                .Select(i => parquetFields[i])
                .ToList();
            if (columnsToUse.Count == 0) throw new ArgumentException("No valid column indices match the Parquet schema.");
            if (options.Verbose) Console.WriteLine($"Mapped column indices from --columns (Parquet): {string.Join(", ", columnsToUse)}");
            return true;
        }

        throw new InvalidOperationException("Column indices specified but no schema available to map them.");
    }

    // Priority 3: Use --config with fallback
    private static bool TryUseConfig(Options options, string fileName, List<string> allColumns, out List<string> columnsToUse)
    {
        columnsToUse = null;
        if (options.PrimaryKeyColumns == null || !options.PrimaryKeyColumns.ContainsKey(fileName)) return false;

        columnsToUse = options.PrimaryKeyColumns[fileName];
        if (columnsToUse.Count == 0)
        {
            if (allColumns == null)
                throw new InvalidOperationException("No primary key columns specified in config and no schema available to default to all columns.");

            columnsToUse = allColumns;
            if (options.Verbose)
            {
                string source = options.IsCsv ? "CSV columns" : "Parquet fields";
                Console.WriteLine($"Using primary key columns from config, but no columns marked as primary key; defaulting to all {source}: {string.Join(", ", columnsToUse)}");
                Program.Log($"Using primary key columns from config, but no columns marked as primary key; defaulting to all {source}: {string.Join(", ", columnsToUse)}");
            }
        }
        else if (options.Verbose)
        {
            Console.WriteLine($"Using primary key columns from config: {string.Join(", ", columnsToUse)}");
        }
        return true;
    }
}



static class CsvOperations
{
    public static DataTable LoadCsv(Options options, List<string> columnsToUse)
    {
        string filePath = options.FilePath;
        char delimiter = options.Delimiter;
        bool hasHeader = options.HasHeader;

        if (options.Verbose) Console.WriteLine($"Loading CSV from {filePath} with delimiter: '{delimiter}'");

        var dataTable = new DataTable();
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = delimiter.ToString(),
            HasHeaderRecord = hasHeader,
            IgnoreBlankLines = true,
            TrimOptions = TrimOptions.Trim,
        };

        //using var reader = new StreamReader(filePath);
        using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read);
        Stream stream = fileStream;

        // Check if the file is gzipped (based on file extension)
        if (filePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
        {
            stream = new GZipStream(fileStream, CompressionMode.Decompress);
        }

        using var reader = new StreamReader(stream);
        using var csv = new CsvReader(reader, config);

        if (!csv.Read()) return dataTable; // No data

        List<int> columnIndicesToLoad;
        string[] headers = null;

        if (hasHeader)
        {
            csv.ReadHeader();
            headers = csv.HeaderRecord;
            if (columnsToUse == null || columnsToUse.Count == 0)
            {
                // Use all columns if columnsToUse is empty or null
                columnIndicesToLoad = Enumerable.Range(0, headers.Length).ToList();
                foreach (string header in headers)
                {
                    dataTable.Columns.Add(header);
                }
            }
            else
            {
                columnIndicesToLoad = columnsToUse
                    .Select(f => Array.IndexOf(headers, f))
                    .Where(i => i >= 0)
                    .ToList();

                if (columnIndicesToLoad.Count < columnsToUse.Count)
                {
                    var missing = columnsToUse.Except(columnIndicesToLoad.Select(i => headers[i])).ToList();
                    Console.WriteLine($"Warning: These columns were not found in CSV headers: {string.Join(", ", missing)}");
                }

                foreach (int index in columnIndicesToLoad)
                {
                    dataTable.Columns.Add(headers[index]);
                }
            }
        }
        else
        {
            int fieldCount = csv.Context.Parser.Count;
            if (columnsToUse == null || columnsToUse.Count == 0)
            {
                // Use all columns with default names if columnsToUse is empty or null
                columnIndicesToLoad = Enumerable.Range(0, fieldCount).ToList();
                for (int i = 0; i < fieldCount; i++)
                {
                    dataTable.Columns.Add($"Column{i}");
                }
            }
            else
            {
                columnIndicesToLoad = Enumerable.Range(0, Math.Min(fieldCount, columnsToUse.Count)).ToList();
                foreach (var column in columnsToUse.Take(columnIndicesToLoad.Count))
                {
                    dataTable.Columns.Add(column);
                }
            }
        }
        do
        {
            var row = dataTable.NewRow();
            for (int i = 0; i < columnIndicesToLoad.Count; i++)
            {
                row[i] = csv.GetField(columnIndicesToLoad[i])?.Trim() ?? "NULL";
            }
            dataTable.Rows.Add(row);
        } while (csv.Read());

        if (options.Verbose) Console.WriteLine($"Loaded {dataTable.Rows.Count} rows from CSV.");
        return dataTable;
    }
}




static class ParquetOperations
{
    public static async Task<DataTable> LoadParquet(Options options, List<string> columnsToUse)
    {
        if (options.Verbose) Console.WriteLine($"Loading Parquet from {options.FilePath}...");

        var parquetEngine = await ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None);
        var availableFields = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();

        List<string> fieldsToLoad;

        if (columnsToUse == null || columnsToUse.Count == 0)
        {
            // If no columns specified, load all available fields
            fieldsToLoad = availableFields;
            if (options.Verbose) Console.WriteLine("No columns specified; loading all available fields.");
        }
        else
        {
            // Filter specified columns against available fields
            fieldsToLoad = columnsToUse
                .Where(f => availableFields.Contains(f, StringComparer.OrdinalIgnoreCase))
                .ToList();
            var invalidFields = columnsToUse.Except(fieldsToLoad, StringComparer.OrdinalIgnoreCase).ToList();

            if (invalidFields.Any())
            {
                Console.WriteLine($"Warning: These fields do not exist in the Parquet file: {string.Join(", ", invalidFields)}");
            }
        }

        if (options.Verbose) Console.WriteLine($"Loading Parquet fields: {string.Join(", ", fieldsToLoad)}");

        var loadResult = await parquetEngine.ReadRowsAsync(
            fieldsToLoad,
            0,
            (int)parquetEngine.RecordCount,
            CancellationToken.None,
            null);

        var dataTable = loadResult.Invoke(false);
        if (options.Verbose) Console.WriteLine($"Loaded {dataTable.Rows.Count} rows from Parquet.");
        return dataTable;
    }
}

static class FileInfoProvider
{
    public static void DisplayFileInfo(Options options)
    {
        Console.WriteLine("══════ File Information ══════");
        Console.WriteLine($"File: {options.FilePath}");
        Console.WriteLine($"Mode: {(options.IsCsv ? "CSV" : "Parquet")}");

        if (options.IsCsv)
        {
            Console.WriteLine($"Delimiter: '{options.Delimiter}' | Header: {options.HasHeader}");
            var fileInfo = new FileInfo(options.FilePath);
            Console.WriteLine($"File Size: {FormatFileSize(fileInfo.Length)}");
        }
        else
        {
            var parquetEngine = ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None).Result;
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
            Console.WriteLine($"Number of Columns: {parquetEngine.Schema.Fields.Count()}");

            var schemaTypes = parquetEngine.Schema.DataFields
                .GroupBy(f => f.SchemaType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);
            Console.WriteLine("Schema Type Summary:");
            foreach (var type in schemaTypes)
            {
                Console.WriteLine($"  {type.Type,-20}: {type.Count} column(s)");
            }

            var fileInfo = new FileInfo(options.FilePath);
            Console.WriteLine($"File Size: {FormatFileSize(fileInfo.Length)}");
        }
        Console.WriteLine("══════════════════════════════");
    }

    public static string FormatFileSize(long bytes)
    {
        string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
        int counter = 0;
        decimal number = bytes;
        while (Math.Round(number / 1024) >= 1)
        {
            number /= 1024;
            counter++;
        }
        return $"{number:n2} {suffixes[counter]}";
    }
}





static class ConfigManager
{
    private const string DefaultConfigFile = "pklist.json";

    public static void ReconfigColumnsFromFile(Options options)
    {
        if (string.IsNullOrEmpty(options.FilePath))
        {
            throw new ArgumentException("File path must be provided.");
        }

        string configFilePath = DefaultConfigFile;
        string fileName = Path.GetFileName(options.FilePath);

        if (options.Verbose) Console.WriteLine($"Reading columns from '{options.FilePath}' to update '{configFilePath}'...");

        // Get columns from the file
        List<string> columns = options.IsCsv
            ? GetCsvColumns(options)
            : GetParquetColumns(options).Result;

        if (columns == null || !columns.Any())
        {
            Console.WriteLine($"No columns found in '{options.FilePath}'. Config not updated.");
            return;
        }

        // Load existing config or create new
        ConfigFile config;
        if (File.Exists(configFilePath))
        {
            string jsonContent = File.ReadAllText(configFilePath);
            config = JsonSerializer.Deserialize(jsonContent, ConfigJsonContext.Default.ConfigFile) ?? new ConfigFile();
            if (config.ParquetFiles == null)
            {
                config.ParquetFiles = new Dictionary<string, ParquetFileConfig>();
            }
        }
        else
        {
            config = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };
        }

        // Get or create entry for the file
        if (!config.ParquetFiles.ContainsKey(fileName))
        {
            config.ParquetFiles[fileName] = new ParquetFileConfig { Columns = new List<ColumnConfig>() };
        }

        var existingColumns = config.ParquetFiles[fileName].Columns ?? new List<ColumnConfig>();
        var existingNames = existingColumns.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

        // Add new columns if not already present
        foreach (var column in columns)
        {
            if (!existingNames.Contains(column))
            {
                existingColumns.Add(new ColumnConfig { Name = column, IsPrimaryKey = false });
                if (options.Verbose) Console.WriteLine($"Added column '{column}' for '{fileName}' with IsPrimaryKey = false.");
            }
            else if (options.Verbose)
            {
                Console.WriteLine($"Column '{column}' already exists for '{fileName}'; skipping.");
            }
        }

        config.ParquetFiles[fileName].Columns = existingColumns;

        // Save updated config
        string updatedJson = JsonSerializer.Serialize(config, ConfigJsonContext.Default.ConfigFile);
        File.WriteAllText(configFilePath, updatedJson);

        if (options.Verbose) Console.WriteLine($"Updated '{configFilePath}' with {columns.Count} columns from '{fileName}'.");
    }

    public static List<string> GetCsvColumns(Options options)
    {
        try
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = options.Delimiter.ToString(),
                HasHeaderRecord = options.HasHeader,
                IgnoreBlankLines = true,
                TrimOptions = TrimOptions.Trim,
            };

            using var fileStream = new FileStream(options.FilePath, FileMode.Open, FileAccess.Read);
            Stream stream = fileStream;

            // Check if the file is gzipped (based on file extension)
            if (options.FilePath.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
            {
                stream = new GZipStream(fileStream, CompressionMode.Decompress);
            }

            using var reader = new StreamReader(stream);            
            using var csv = new CsvReader(reader, config);

            if (!csv.Read()) return new List<string>();

            if (options.HasHeader)
            {
                csv.ReadHeader();
                return csv.HeaderRecord.ToList();
            }
            else
            {
                int fieldCount = csv.Context.Parser.Count;
                return Enumerable.Range(0, fieldCount).Select(i => $"Column_{i}").ToList();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading CSV columns from '{options.FilePath}': {ex.Message}");
            return null;
        }
    }

    public static async Task<List<string>> GetParquetColumns(Options options)
    {
        try
        {
            var parquetEngine = await ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None);
            return parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading Parquet columns from '{options.FilePath}': {ex.Message}");
            return null;
        }
    }
}
