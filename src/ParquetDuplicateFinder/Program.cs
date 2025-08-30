using CsvHelper.Configuration;
using CsvHelper;
using ParquetViewer.Engine.Exceptions;
using System.Data;
using System.Globalization;
using ParquetViewer.Engine;
using System.Text;
using System.Security.Cryptography;
using System.IO.Compression;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml;
using ExcelDataReader;

namespace ParquetDuplicateFinder;

partial class Program
{
    private static string? logFilePath;
    static async Task Main(string[] args)
    {
        int FileCount = 1;
        int DupFileCount = 0;
        try
        {
            InitializeLogging(new(), args);            
            var options = CommandLineParser.Parse(args);
            if (options == null) return;
            
            var filesToProcess = options.IsFolderMode
                ? Directory.EnumerateFiles(options.FolderPath, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase))
                : new[] { options.FilePath };

            if (options.IsFolderMode)
            {
                Log($"Processing For Folder : {options.FolderPath}");
                Log($"Processing {filesToProcess.ToList().Count} files.");
                LogLineBreak();
            }

            foreach (var filePath in filesToProcess)
            {
                Log($"Processing file : {Path.GetFileName(filePath)}");

                if (!Program.IsFileAvailable(filePath))
                    continue;

                UpdateOptionsForFile(options, filePath);

                if (options.ReconfigColumns)
                {
                    ConfigManagerExcel.ReconfigColumnsFromFile(options);
                    Log("Updated pklist.xlsx with file columns.");
                    continue;
                }

                // Determine columns to use based on updated options
                List<string> columnsToUse = await GetColumnsToUse(options);
                
                if (columnsToUse == null)
                {
                    Log($"Skipping file '{filePath}' due to schema mismatch.");
                    continue; // Skip to next file
                }

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
                    if(LogDuplicates(dataTable, columnsToUse, options))
                    {
                        DupFileCount++;
                    };
                }

                Log($"Finished processing file: {Path.GetFileName(filePath)} | ({FileCount++})");
                LogLineBreak();
            }

            if(DupFileCount > 0) Log($"{DupFileCount} parquet file(s) contains Duplicates records.");
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
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = options.Delimiter.ToString(),
                HasHeaderRecord = options.HasHeader
            });

            if (csv.Read() && options.HasHeader)
            {
                csv.ReadHeader();
                var columns = ColumnSelector.GetColumnsToUse(options, csv.HeaderRecord);
                return columns; // Will return null if skipping
            }
            return ColumnSelector.GetColumnsToUse(options); // Will return null if skipping
        }
        else
        {
            var parquetEngine = await ParquetOperations.LoadParquet(options, new List<string> { });
            var availableFields = parquetEngine.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
            return ColumnSelector.GetColumnsToUse(options, null, availableFields); // Will return null if skipping
        }
    }
    private static void InitializeLogging(Options options, string[] args)
    {
        Directory.CreateDirectory(options.LogFolder);
        string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        logFilePath = Path.Combine(options.LogFolder, $"Run_{timestamp}.log");
        Log("Starting ParquetDuplicateFinder with args :");
        foreach (string arg in args) Log($"{arg} ");
        LogLineBreak();
    }

    public static void Log(string message, bool newLine = true)
    {
        //Console.WriteLine(message);
        File.AppendAllText(logFilePath, $"{DateTime.Now}: {message}{(newLine ? "\n" : string.Empty)}");
    }
    public static void LogLineBreak() => File.AppendAllText(logFilePath, Environment.NewLine);
    

    private static void LogFileInfo(Options options)
    {
        Log("══════ File Information ══════");
        Log($"File: [{(options.IsCsv ? "CSV" : "Parquet")}] {Path.GetFileName(options.FilePath)}");

        var fileInfo = new FileInfo(options.FilePath);
        if (options.IsCsv)
        {
            Log($"Delimiter: '{options.Delimiter}' | Header: {options.HasHeader} | File Size: {FileInfoProvider.FormatFileSize(fileInfo.Length)}");
        }
        else
        {
            var parquetEngine = ParquetEngine.OpenFileOrFolderAsync(options.FilePath, CancellationToken.None).Result;
            Log($"Total Records: {parquetEngine.RecordCount:N0} | Number of Columns: {parquetEngine.Schema.Fields.Count()} | File Size: {FileInfoProvider.FormatFileSize(fileInfo.Length)}");
            Log("Schema Type Summary:");
            var schemaTypes = parquetEngine.Schema.DataFields
                .GroupBy(f => f.SchemaType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);
            foreach (var type in schemaTypes)
            {
                Log($"  {type.Type,-20}: {type.Count} column(s)");
            }
        }
    }

    private static void LogColumnStats(DataTable dataTable, Options options)
    {
        if (options.Verbose) Log("Displaying column statistics...");
        Log("══════ Column Details ════════");
        Log("All Available Columns:");
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            Log($"  {i,2}. {dataTable.Columns[i].ColumnName}");
        }
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

    private static bool LogDuplicates(DataTable dataTable, List<string> columnsToUse, Options options)
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
            Log($"[{Path.GetFileName(options.FilePath)}] : No duplicates found.");
            return false;
        }

        Log($"Found {duplicates.Count} duplicate groups.");
        Log("═══════ Duplicate Summary ══════");
        Log($"{"Group #",-8} | {"Count",-6} | {"Row #",-12} | Record");
        Log(new string('─', 8 + 3 + 6 + 3 + 12 + 3 + columnsToUse.Count * 20));

        int displayLimit = options.Limit > 0 ? Math.Min(options.Limit, duplicates.Count) : duplicates.Count;
        for (int i = 0; i < displayLimit; i++)
        {
            var group = duplicates[i];
            var sample = group.Value[0];
            var sampleValues = string.Join(" | ", columnsToUse
                .Select(c => (sample.Row[c]?.ToString() ?? "NULL").PadRight(36)[..Math.Min(36, (sample.Row[c]?.ToString() ?? "").Length)]));
            Log($"{i + 1,-8} | {group.Value.Count,-6} | {sample.Position,-12} | {sampleValues}");
        }

        int totalDuplicates = duplicates.Sum(g => g.Value.Count - 1);
        Log($"\nSummary: Found {totalDuplicates} duplicate records in {duplicates.Count} groups.");
        LogLineBreak();

        return true;
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
            //JSON Config
            //string jsonContent = File.ReadAllText(configFilePath);
            //var config = JsonSerializer.Deserialize(jsonContent, ConfigJsonContext.Default.ConfigFile);

            // Use ConfigManager's LoadFromExcel to read the Excel file
            var config = ConfigManagerExcel.LoadFromExcel(configFilePath);

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

    private static void LogMissingConfigAndFiles(Options options)
    {
        Log("══════ Configuration and File Comparison ══════");
        string configFilePath = options.ConfigFilePath;
        string folderPath = options.IsFolderMode ? options.FolderPath : Path.GetDirectoryName(options.FilePath);

        if (string.IsNullOrEmpty(folderPath))
        {
            Log("Error: Could not determine folder path.");
            return;
        }

        // 1. Get all Parquet files in the folder
        var parquetFiles = Directory.EnumerateFiles(folderPath, "*.*", SearchOption.TopDirectoryOnly)
            .Where(f => f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                        f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase))
            .Select(Path.GetFileName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // 2. Load the configuration
        var config = ConfigManagerExcel.LoadFromExcel(configFilePath);
        if (config == null || config.ParquetFiles == null)
        {
            Log($"Error: Could not load or find config file '{configFilePath}'.");
            Log("═══════════════════════════════════════════════");
            return;
        }

        var configEntries = config.ParquetFiles.Keys.ToHashSet(StringComparer.OrdinalIgnoreCase);

        // 3. Find files in the folder that are missing a config entry
        var filesMissingConfig = parquetFiles.Except(configEntries).ToList();

        if (filesMissingConfig.Any())
        {
            Log("Files found in folder but missing in pklist.xlsx:");
            foreach (var file in filesMissingConfig)
            {
                Log($"  - {file}");
            }
        }
        else
        {
            Log("All Parquet files in the folder have an entry in pklist.xlsx.");
        }

        LogLineBreak();

        // 4. Find config entries that are missing a corresponding file
        var configMissingFiles = configEntries.Except(parquetFiles).ToList();

        if (configMissingFiles.Any())
        {
            Log("Entries in pklist.xlsx but missing a corresponding file in the folder:");
            foreach (var entry in configMissingFiles)
            {
                Log($"  - {entry}");
            }
        }
        else
        {
            Log("All entries in pklist.xlsx correspond to a file in the folder.");
        }

        Log("═══════════════════════════════════════════════");
    }
}
static class CommandLineParser
{
    private const string DefaultConfigFile = "pklist.xlsx";
    private static readonly HashSet<string> ValidArgs = new HashSet<string>
    {
        "--folder",
        "--csv",
        "--delimiter",
        "--header",
        "-f", "--fields",
        "-c", "--columns","--allColumns",
        "-v", "--verbose",
        "-l", "--limit",
        "-d", "--findDuplicates",
        "-pf", "--printData",
        "-s", "--stats",
        "-h", "--help"
        ,"--config"
        ,"--reconfigColumns"
        ,"--checkConfig"
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
        if (options.CheckConfigAndFiles) return options;

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
                case "--allColumns":
                    options.AllColumns = true;
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
                case "--checkConfig":
                    options.CheckConfigAndFiles = true;
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
            Console.WriteLine("  --config <path>                   Path to Excel config file with PK columns");
            Console.WriteLine("  --csv                             Process a CSV file instead of Parquet");
            Console.WriteLine("  --delimiter <char>                CSV delimiter (fixed to ',' for folder mode)");
            Console.WriteLine("  --header                          Treat first CSV row as header (always true for CSV)");
            Console.WriteLine("  --allColumns                      All Fields to check for duplicates");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Fields to check for duplicates");
            Console.WriteLine("  -c, --columns <index1,index2,...> Column indices for duplicates");
            Console.WriteLine("  -v, --verbose                     Show detailed output");
            Console.WriteLine("  -l, --limit <number>              Limit duplicate groups displayed");
            Console.WriteLine("  -d, --findDuplicates              Find and display duplicates");
            Console.WriteLine("  -pf, --printData                  Print file data");
            Console.WriteLine("  -s, --stats                       Show column statistics");
            Console.WriteLine("  --reconfigColumns                 Update pklist.xlsx with file columns");
            Console.WriteLine("  --checkConfig                     Check pklist.xlsx with file columns");
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
    public string FolderPath { get; set; } 
    public bool IsFolderMode { get; set; } 
    public string LogFolder { get; set; } = "Logs";
    public List<string> Fields { get; set; }
    public List<int> ColumnIndices { get; set; }
    public bool AllColumns { get; set; }
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
    public bool CheckConfigAndFiles { get; set; } = false;
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

static class ColumnSelector
{
    public static List<string> GetColumnsToUse(Options options, string[] csvHeaders = null, List<string> parquetFields = null)
    {
        string fileName = Path.GetFileName(options.FilePath);
        List<string> allColumns = GetAllColumns(options, csvHeaders, parquetFields);

        if (options.AllColumns)
        {
            if (allColumns != null)
            {
                if (options.Verbose)
                {
                    string source = options.IsCsv ? "CSV columns" : "Parquet fields";
                    Console.WriteLine($"Using all {source}: {string.Join(", ", allColumns)}");
                    Program.Log($"Using all {source}: {string.Join(", ", allColumns)}");
                }
                return allColumns;
            }
        }

        if (TryUseFields(options, out var columnsToUse)) return columnsToUse;
        if (TryUseColumnIndices(options, csvHeaders, parquetFields, out columnsToUse)) return columnsToUse;

        if (TryUseConfig(options, fileName, allColumns, out columnsToUse))
        {
            if (columnsToUse == null) // Skip file due to schema mismatch
            {
                return null;
            }
            return columnsToUse;
        }

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
        if (allColumns == null)
        {
            throw new InvalidOperationException("No schema available to validate config columns against.");
        }

        //columnsToUse = ValidateConfigSchema(options, allColumns, columnsToUse);

        //if (columnsToUse == null) // Schema mismatch, skip file
        //{
        //    return false;
        //}

        if (columnsToUse.Count == 0)
        {
            columnsToUse = allColumns;
            string source = options.IsCsv ? "CSV columns" : "Parquet fields";
            Program.Log($"Using primary key columns from config, but no columns specified; defaulting to all {source}: {string.Join(", ", columnsToUse)}");
            if (options.Verbose) Console.WriteLine($"Using primary key columns from config, but no columns specified; defaulting to all {source}: {string.Join(", ", columnsToUse)}");
        }
        else
        {
            if (options.Verbose) Console.WriteLine($"Using primary key columns from config: {string.Join(", ", columnsToUse)}");
            Program.Log($"Using primary key columns from config: {string.Join(", ", columnsToUse)}");
        }
        return true;
    }
    
    private static List<string> ValidateConfigSchema(Options options, List<string> allColumns, List<string> configColumns)
    {
        var missingFromSchema = configColumns.Except(allColumns).ToList();
        var missingFromConfig = allColumns.Except(configColumns).ToList();

        bool hasMismatch = missingFromSchema.Count > 0 || missingFromConfig.Count > 0;

        if (hasMismatch)
        {
            string source = options.IsCsv ? "CSV" : "Parquet";
            var messages = new List<string>();

            if (missingFromSchema.Count > 0)
            {
                messages.Add($"Config contains columns not found in {source} schema: {string.Join(", ", missingFromSchema)}");
            }

            if (missingFromConfig.Count > 0)
            {
                messages.Add($"Columns in {source} schema not specified in config: {string.Join(", ", missingFromConfig)}");
            }

            string fullMessage = $"Schema mismatch detected between config and {source} file:\n" +
                               string.Join("\n", messages) +
                               $"\nAvailable columns: {string.Join(", ", allColumns)}" +
                               $"\nConfig columns: {string.Join(", ", configColumns)}" +
                               "\nSkipping this file due to schema mismatch.";

            if (options.Verbose)
            {
                Console.WriteLine(fullMessage);
                Program.Log(fullMessage);
            }

            //return null; // Signal to skip the file
        }

        return configColumns; // No mismatch, return original config columns
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
            BadDataFound = context => Program.Log($"Skipped Bad data found with RowContents: {context.RawRecord}", false)
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
        try
        {
            do
            {
                var row = dataTable.NewRow();
                for (int i = 0; i < columnIndicesToLoad.Count; i++)
                {
                    row[i] = csv.GetField(columnIndicesToLoad[i])?.Trim() ?? "NULL";
                }
                dataTable.Rows.Add(row);
            } while (csv.Read());
        }
        catch (Exception ex)
        {
            if(options.Verbose)Console.WriteLine(ex.Message);
            Program.Log(ex.Message);
            throw ;
        }
        

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
        Console.WriteLine($"File: [{(options.IsCsv ? "CSV" : "Parquet")}] {Path.GetFileName(options.FilePath)}");

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


static class ConfigManagerExcel
{
    private const string DefaultConfigFile = "pklist.xlsx";

    static ConfigManagerExcel()
    {
        // Register code pages encoding to support Windows-1252
        System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
    }

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

        // Load existing config from Excel or create new
        ConfigFile existingConfig = LoadFromExcel(configFilePath) ?? new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };
        ConfigFile newConfig = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>(existingConfig.ParquetFiles) };

        // Get or create entry for the file
        if (!newConfig.ParquetFiles.ContainsKey(fileName))
        {
            newConfig.ParquetFiles[fileName] = new ParquetFileConfig { Columns = new List<ColumnConfig>() };
        }

        var existingColumns = newConfig.ParquetFiles[fileName].Columns ?? new List<ColumnConfig>();
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

        newConfig.ParquetFiles[fileName].Columns = existingColumns;

        // Save updated config to Excel, preserving existing data
        SaveToExcel(newConfig, configFilePath);

        if (options.Verbose) Console.WriteLine($"Updated '{configFilePath}' with {columns.Count} columns from '{fileName}'.");
    }

    public static ConfigFile LoadFromExcel(string filePath)
    {
        if (!File.Exists(filePath))
        {
            return null;
        }

        try
        {
            using var stream = File.Open(filePath, FileMode.Open, FileAccess.Read);
            using var reader = ExcelReaderFactory.CreateReader(stream);
            var result = reader.AsDataSet(new ExcelDataSetConfiguration()
            {
                ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                {
                    UseHeaderRow = true
                }
            });

            var table = result.Tables[0];
            var config = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };

            foreach (DataRow row in table.Rows)
            {
                string parquetName = row["ParquetName"]?.ToString();
                string columnName = row["ParquetColumns"]?.ToString();
                bool isPrimaryKey = bool.TryParse(row["isPrimaryKeyColumn"]?.ToString(), out bool value) && value;

                if (string.IsNullOrEmpty(parquetName) || string.IsNullOrEmpty(columnName))
                    continue;

                if (!config.ParquetFiles.ContainsKey(parquetName))
                {
                    config.ParquetFiles[parquetName] = new ParquetFileConfig { Columns = new List<ColumnConfig>() };
                }

                // Only add if not already present to avoid duplicates
                var columns = config.ParquetFiles[parquetName].Columns;
                if (!columns.Any(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase)))
                {
                    columns.Add(new ColumnConfig
                    {
                        Name = columnName,
                        IsPrimaryKey = isPrimaryKey
                    });
                }
            }

            return config;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error reading Excel file '{filePath}': {ex.Message}");
            return null;
        }
    }

    private static void SaveToExcel(ConfigFile newConfig, string filePath)
    {
        try
        {
            // Load existing data first to preserve anything not in current config
            ConfigFile existingConfig = LoadFromExcel(filePath) ?? new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };

            // Merge new config with existing config
            foreach (var kvp in newConfig.ParquetFiles)
            {
                if (!existingConfig.ParquetFiles.ContainsKey(kvp.Key))
                {
                    existingConfig.ParquetFiles[kvp.Key] = kvp.Value;
                }
                else
                {
                    var existingColumns = existingConfig.ParquetFiles[kvp.Key].Columns ?? new List<ColumnConfig>();
                    var newColumns = kvp.Value.Columns ?? new List<ColumnConfig>();
                    var existingNames = existingColumns.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);

                    foreach (var column in newColumns)
                    {
                        if (!existingNames.Contains(column.Name))
                        {
                            existingColumns.Add(column);
                        }
                        else
                        {
                            // Update existing column's IsPrimaryKey if it differs
                            var existingColumn = existingColumns.First(c => c.Name.Equals(column.Name, StringComparison.OrdinalIgnoreCase));
                            existingColumn.IsPrimaryKey = column.IsPrimaryKey;
                        }
                    }
                    existingConfig.ParquetFiles[kvp.Key].Columns = existingColumns;
                }
            }

            // If file exists, update it; otherwise create new
            if (File.Exists(filePath))
            {
                using var spreadsheetDocument = SpreadsheetDocument.Open(filePath, true);
                var workbookPart = spreadsheetDocument.WorkbookPart;
                var worksheetPart = workbookPart.WorksheetParts.First();
                var sheetData = worksheetPart.Worksheet.GetFirstChild<SheetData>();

                // Clear existing data (except header)
                var rows = sheetData.Elements<Row>().ToList();
                foreach (var row in rows.Skip(1)) // Skip header row
                {
                    row.Remove();
                }

                // Add all data rows from merged config
                foreach (var file in existingConfig.ParquetFiles)
                {
                    foreach (var column in file.Value.Columns ?? new List<ColumnConfig>())
                    {
                        var row = new Row();
                        row.Append(
                            new Cell() { CellValue = new CellValue(file.Key), DataType = CellValues.String },
                            new Cell() { CellValue = new CellValue(column.Name), DataType = CellValues.String },
                            new Cell() { CellValue = new CellValue(column.IsPrimaryKey.ToString()), DataType = CellValues.String }
                        );
                        sheetData.Append(row);
                    }
                }

                worksheetPart.Worksheet.Save();
                workbookPart.Workbook.Save();
            }
            else
            {
                // Create new file if it doesn't exist
                using var spreadsheetDocument = SpreadsheetDocument.Create(filePath, SpreadsheetDocumentType.Workbook);

                var workbookPart = spreadsheetDocument.AddWorkbookPart();
                workbookPart.Workbook = new Workbook();

                var worksheetPart = workbookPart.AddNewPart<WorksheetPart>();
                worksheetPart.Worksheet = new Worksheet(new SheetData());

                var sheets = spreadsheetDocument.WorkbookPart.Workbook.AppendChild(new Sheets());
                var sheet = new Sheet()
                {
                    Id = spreadsheetDocument.WorkbookPart.GetIdOfPart(worksheetPart),
                    SheetId = 1,
                    Name = "Sheet1"
                };
                sheets.Append(sheet);

                var sheetData = worksheetPart.Worksheet.GetFirstChild<SheetData>();

                // Add header row
                var headerRow = new Row();
                headerRow.Append(
                    new Cell() { CellValue = new CellValue("ParquetName"), DataType = CellValues.String },
                    new Cell() { CellValue = new CellValue("ParquetColumns"), DataType = CellValues.String },
                    new Cell() { CellValue = new CellValue("isPrimaryKeyColumn"), DataType = CellValues.String }
                );
                sheetData.Append(headerRow);

                // Add all data rows from merged config
                foreach (var file in existingConfig.ParquetFiles)
                {
                    foreach (var column in file.Value.Columns ?? new List<ColumnConfig>())
                    {
                        var row = new Row();
                        row.Append(
                            new Cell() { CellValue = new CellValue(file.Key), DataType = CellValues.String },
                            new Cell() { CellValue = new CellValue(column.Name), DataType = CellValues.String },
                            new Cell() { CellValue = new CellValue(column.IsPrimaryKey.ToString()), DataType = CellValues.String }
                        );
                        sheetData.Append(row);
                    }
                }

                workbookPart.Workbook.Save();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error writing to Excel file '{filePath}': {ex.Message}");
        }
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

    public static async Task CompareConfigWithParquetFiles(string folderPath, string configFilePath)
    {
        // 1. Load the Excel configuration
        var config = LoadFromExcel(configFilePath);
        if (config == null || config.ParquetFiles == null)
        {
            Console.WriteLine($"Error: Could not load or find config file '{configFilePath}'.");
            return;
        }

        // 2. Iterate through the Parquet files
        var parquetFiles = Directory.EnumerateFiles(folderPath, "*.parquet", SearchOption.TopDirectoryOnly)
                                    .ToList();

        foreach (var filePath in parquetFiles)
        {
            string fileName = Path.GetFileName(filePath);
            Console.WriteLine($"\nComparing file: {fileName}");

            // 3. Get the schema for the file
            List<string> parquetColumns;
            try
            {
                var parquetEngine = await ParquetViewer.Engine.ParquetEngine.OpenFileOrFolderAsync(filePath, CancellationToken.None);
                parquetColumns = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Error reading schema from {fileName}: {ex.Message}");
                continue;
            }

            // 4. Perform the comparison
            if (config.ParquetFiles.ContainsKey(fileName))
            {
                var configColumns = config.ParquetFiles[fileName].Columns;
                if (configColumns == null)
                {
                    Console.WriteLine($"  Warning: No columns defined for '{fileName}' in config.");
                    continue;
                }

                var configColumnNames = configColumns.Select(c => c.Name).ToList();

                // Columns in Parquet but not in config
                var missingInConfig = parquetColumns.Except(configColumnNames, StringComparer.OrdinalIgnoreCase).ToList();
                if (missingInConfig.Any())
                {
                    Console.WriteLine($"  - Columns in file '{fileName}' but missing in config: {string.Join(", ", missingInConfig)}");
                }

                // Columns in config but not in Parquet
                var missingInParquet = configColumnNames.Except(parquetColumns, StringComparer.OrdinalIgnoreCase).ToList();
                if (missingInParquet.Any())
                {
                    Console.WriteLine($"  - Columns in config but missing in file '{fileName}': {string.Join(", ", missingInParquet)}");
                }

                // You cannot automate the isPrimaryKeyColumn check without another data source, but you can report on it.
                // This is a placeholder for your manual action step.
            }
            else
            {
                Console.WriteLine($"  - No entry found for '{fileName}' in the config file. All columns are 'missing'.");
            }
        }
    }


}