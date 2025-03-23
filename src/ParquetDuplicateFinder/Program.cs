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

namespace ParquetDuplicateFinder;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            var options = CommandLineParser.Parse(args);
            if (options == null) return;

            List<string> columnsToUse;

            if (options.IsCsv)
            {
                using var reader = new StreamReader(options.FilePath);
                using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture) { Delimiter = options.Delimiter.ToString(), HasHeaderRecord = options.HasHeader });
                if (csv.Read() && options.HasHeader)
                {
                    csv.ReadHeader();
                    columnsToUse = ColumnSelector.GetColumnsToUse(options, csv.HeaderRecord);
                }
                else if (!options.HasHeader && (options.ColumnIndices != null || options.Fields == null))
                {
                    columnsToUse = ColumnSelector.GetColumnsToUse(options, Enumerable.Range(0, csv.Context.Parser.Count).Select(i => $"Column_{i}").ToArray());
                }
                else
                {
                    columnsToUse = ColumnSelector.GetColumnsToUse(options);
                }
            }
            else
            {
                var parquetEngine = await ParquetOperations.LoadParquet(options, new List<string> { }); // Load minimal to get schema
                var availableFields = parquetEngine.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
                columnsToUse = ColumnSelector.GetColumnsToUse(options, null, availableFields);
            }

            if (options.Verbose || options.ShowStats)
            {
                FileInfoProvider.DisplayFileInfo(options);
            }

            DataTable dataTable = options.IsCsv
                ? CsvOperations.LoadCsv(options, columnsToUse)
                : await ParquetOperations.LoadParquet(options, columnsToUse);

            if (options.ShowStats)
            {
                DataProcessor.ShowColumnStats(dataTable, options.ColumnIndices, options.Verbose);
            }

            if (options.PrintData)
            {
                DataProcessor.PrintData(dataTable, options.RowLimit, options.Verbose);
            }

            if (options.FindDuplicates)
            {
                DataProcessor.FindDuplicates(dataTable, columnsToUse, options.Verbose, options.Limit);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
            if (ex is AllFilesSkippedException afse) Console.WriteLine($"Details: {afse.Message}");
            else if (ex is SomeFilesSkippedException sfse) Console.WriteLine($"Details: {sfse.Message}");
            else if (ex is FileReadException fre) Console.WriteLine($"Details: {fre.Message}");
            else if (ex is MultipleSchemasFoundException msfe) Console.WriteLine($"Details: {msfe.Message}");
        }
    }
}



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

        if (!configSpecified && File.Exists(DefaultConfigFile))
        {
            options.ConfigFilePath = DefaultConfigFile;
        }

        if (options.Fields == null && options.ColumnIndices == null && !string.IsNullOrEmpty(options.ConfigFilePath))
        {
            options.PrimaryKeyColumns = LoadPrimaryKeyColumns(options.ConfigFilePath, options.FilePath);
        }

        if (options.Fields != null && (options.ColumnIndices != null || options.ConfigFilePath != null))
        {
            Console.WriteLine("Warning: '--fields' specified; ignoring '--config' and '--columns'.");
        }
        else if (options.ColumnIndices != null && options.ConfigFilePath != null)
        {
            Console.WriteLine("Warning: '--columns' specified; ignoring '--config'.");
        }

        return options;
    }

    private static Dictionary<string, List<string>> LoadPrimaryKeyColumns(string configFilePath, string targetFilePath)
    {
        try
        {
            string jsonContent = File.ReadAllText(configFilePath);
            var config = JsonSerializer.Deserialize(jsonContent, ConfigJsonContext.Default.ConfigFile);

            if (config == null || config.ParquetFiles == null)
            {
                Console.WriteLine($"Config file '{configFilePath}' is empty or missing 'ParquetFiles'.");
                return null;
            }

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
            return pkColumns;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error loading config file '{configFilePath}': {ex.Message}");
            return null;
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("ParquetDuplicateFinder - Find duplicates in Parquet or CSV files");
        Console.WriteLine("\nUsage:");
        Console.WriteLine("  ParquetDuplicateFinder <file_path> [options]");
        Console.WriteLine("\nOptions:");
        Console.WriteLine("  --config <path>                   Path to JSON config file with PK columns");
        Console.WriteLine("  --csv                             Process a CSV file instead of Parquet");
        Console.WriteLine("  --delimiter <char>                CSV delimiter (default: ',')");
        Console.WriteLine("  --header                          Treat first CSV row as header");
        Console.WriteLine("  -f, --fields <field1,field2,...>  Fields to check for duplicates");
        Console.WriteLine("  -c, --columns <index1,index2,...> Column indices for duplicates");
        Console.WriteLine("  -v, --verbose                     Show detailed output");
        Console.WriteLine("  -l, --limit <number>              Limit duplicate groups displayed");
        Console.WriteLine("  -d, --findDuplicates              Find and display duplicates");
        Console.WriteLine("  -pf, --printData                  Print file data");
        Console.WriteLine("  -s, --stats                       Show column statistics");
        Console.WriteLine("  -h, --help                        Show this help message");
    }
}

public class Options
{
    public string FilePath { get; set; }
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

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)]
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
        List<string> columnsToUse = null;
        string fileName = Path.GetFileName(options.FilePath);

        // Priority 1: --fields
        if (options.Fields != null && options.Fields.Count > 0)
        {
            columnsToUse = options.Fields;
            if (options.Verbose) Console.WriteLine($"Using fields from --fields: {string.Join(", ", columnsToUse)}");
        }
        // Priority 2: --columns
        else if (options.ColumnIndices != null && options.ColumnIndices.Count > 0)
        {
            if (options.IsCsv && csvHeaders != null)
            {
                columnsToUse = options.ColumnIndices
                    .Where(i => i >= 0 && i < csvHeaders.Length)
                    .Select(i => csvHeaders[i])
                    .ToList();
                if (columnsToUse.Count == 0)
                {
                    throw new ArgumentException("No valid column indices match the CSV headers.");
                }
                if (options.Verbose) Console.WriteLine($"Mapped column indices from --columns (CSV): {string.Join(", ", columnsToUse)}");
            }
            else if (!options.IsCsv && parquetFields != null)
            {
                columnsToUse = options.ColumnIndices
                    .Where(i => i >= 0 && i < parquetFields.Count)
                    .Select(i => parquetFields[i])
                    .ToList();
                if (columnsToUse.Count == 0)
                {
                    throw new ArgumentException("No valid column indices match the Parquet schema.");
                }
                if (options.Verbose) Console.WriteLine($"Mapped column indices from --columns (Parquet): {string.Join(", ", columnsToUse)}");
            }
            else
            {
                throw new InvalidOperationException("Column indices specified but no schema available to map them.");
            }
        }
        // Priority 3: --config
        else if (options.PrimaryKeyColumns != null && options.PrimaryKeyColumns.ContainsKey(fileName))
        {
            columnsToUse = options.PrimaryKeyColumns[fileName];
            if (options.Verbose) Console.WriteLine($"Using primary key columns from config: {string.Join(", ", columnsToUse)}");
        }
        // Priority 4: All columns
        else if (options.IsCsv && csvHeaders != null)
        {
            columnsToUse = csvHeaders.ToList();
            if (options.Verbose) Console.WriteLine($"No specific columns provided; using all CSV columns: {string.Join(", ", columnsToUse)}");
        }
        else if (!options.IsCsv && parquetFields != null)
        {
            columnsToUse = parquetFields;
            if (options.Verbose) Console.WriteLine($"No specific columns provided; using all Parquet fields: {string.Join(", ", columnsToUse)}");
        }
        else
        {
            throw new InvalidOperationException("No columns specified and no schema available to default to all columns.");
        }

        return columnsToUse;
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

        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, config);

        if (!csv.Read()) return dataTable; // No data

        List<int> columnIndicesToLoad;
        string[] headers = null;

        if (hasHeader)
        {
            csv.ReadHeader();
            headers = csv.HeaderRecord;
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
        else
        {
            int fieldCount = csv.Context.Parser.Count;
            columnIndicesToLoad = Enumerable.Range(0, Math.Min(fieldCount, columnsToUse.Count)).ToList();
            foreach (var column in columnsToUse.Take(columnIndicesToLoad.Count))
            {
                dataTable.Columns.Add(column);
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




static class DataProcessor
{
    public static void PrintData(DataTable dataTable, long rowLimit, bool verbose)
    {
        if (verbose) Console.WriteLine("Printing data...");

        // Set an upper bound for column widths
        const int MAX_COLUMN_WIDTH = 40;
        const int SAMPLE_SIZE = 100; // Sample first 100 rows

        // Calculate column widths based on header and sampled data
        int[] columnWidths = new int[dataTable.Columns.Count];
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            string header = dataTable.Columns[i].ColumnName;
            int sampleWidth = dataTable.Rows.Cast<DataRow>()
                .Take(Math.Min(SAMPLE_SIZE, dataTable.Rows.Count)) // Sample up to 100 rows
                .Select(r => (r[i]?.ToString() ?? "NULL").Length)
                .DefaultIfEmpty(0)
                .Max();
            columnWidths[i] = Math.Min(Math.Max(header.Length, sampleWidth), MAX_COLUMN_WIDTH);
        }

        // Print header
        Console.WriteLine(string.Join(" | ", dataTable.Columns.Cast<DataColumn>()
            .Select((c, i) => c.ColumnName.PadRight(columnWidths[i]))));
        Console.WriteLine(new string('─', columnWidths.Sum() + (dataTable.Columns.Count - 1) * 3));

        // Print rows
        long rowCount = 0;
        foreach (DataRow row in dataTable.Rows)
        {
            if (rowLimit == -1 || rowCount < rowLimit)
            {
                Console.WriteLine(string.Join(" | ", dataTable.Columns.Cast<DataColumn>()
                    .Select((c, i) =>
                    {
                        string value = row[c]?.ToString() ?? "NULL";
                        return value.Length > columnWidths[i]
                            ? value[..columnWidths[i]] // Truncate to column width
                            : value.PadRight(columnWidths[i]); // Pad to column width
                    })));
                rowCount++;
            }
            else break;
        }

        if (verbose) Console.WriteLine($"Printed {rowCount} rows.");
    }

    public static void FindDuplicates(DataTable dataTable, List<string> columnsToUse, bool verbose, int limit)
    {
        if (verbose) Console.WriteLine($"Finding duplicates using columns: {string.Join(", ", columnsToUse)}");

        // Store duplicate groups with row positions
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
            Interlocked.Increment(ref rowPosition); // Thread-safe position increment
        });

        // Filter to only duplicate groups (count > 1)
        var duplicates = duplicateGroups
            .Where(g => g.Value.Count > 1)
            .OrderByDescending(g => g.Value.Count) // Sort by group size
            .ToList();

        if (duplicates.Count == 0)
        {
            Console.WriteLine("No duplicates found.");
            return;
        }

        Console.WriteLine($"Found {duplicates.Count} duplicate groups.");

        // Display limited number of groups
        int displayLimit = limit > 0 ? Math.Min(limit, duplicates.Count) : duplicates.Count;
        Console.WriteLine("═════ Duplicate Summary ═════");
        Console.WriteLine($"{"Group #",-8} | {"Count",-6} | {"Row #",-12} | Record");
        Console.WriteLine(new string('─', 8 + 3 + 6 + 3 + 12 + 3 + columnsToUse.Count * 20)); // Rough estimate for width

        for (int i = 0; i < displayLimit; i++)
        {
            var group = duplicates[i];
            var sample = group.Value[0]; // First record as sample
            var sampleValues = string.Join(" | ", columnsToUse
                .Select(c => (sample.Row[c]?.ToString() ?? "NULL").PadRight(20)[..Math.Min(20, (sample.Row[c]?.ToString() ?? "").Length)]));
            Console.WriteLine($"{i + 1,-8} | {group.Value.Count,-6} | {sample.Position,-12} | {sampleValues}");
        }

        int totalDuplicates = duplicates.Sum(g => g.Value.Count - 1);
        Console.WriteLine($"\nSummary: Found {totalDuplicates} duplicate records in {duplicates.Count} groups.");
    }

    public static void ShowColumnStats(DataTable dataTable, List<int> columnIndices, bool verbose)
    {
        if (verbose) Console.WriteLine("Displaying column statistics...");

        Console.WriteLine("═════ Column Details ═════");
        Console.WriteLine("All Available Columns:");
        for (int i = 0; i < dataTable.Columns.Count; i++)
        {
            Console.WriteLine($"  {i,2}. {dataTable.Columns[i].ColumnName}");
        }
        Console.WriteLine("═════════════════════════");
    }

    private static string CreateKey(DataRow row, List<string> columnsToUse)
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

    private static string FormatFileSize(long bytes)
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
