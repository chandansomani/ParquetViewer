using CsvHelper.Configuration;
using CsvHelper;
using System.Data;
using System.Globalization;
using System.Text.Json;
using ParquetViewer.Engine;
using System.IO.Compression;
using System.Text.Json.Serialization;

namespace ParquetDuplicateFinder;


[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true, WriteIndented = true)]
[JsonSerializable(typeof(ConfigFile))]
[JsonSerializable(typeof(ParquetFileConfig))]
[JsonSerializable(typeof(ColumnConfig))]
[JsonSerializable(typeof(Dictionary<string, ParquetFileConfig>))]
[JsonSerializable(typeof(List<ColumnConfig>))]
public partial class ConfigJsonContext : JsonSerializerContext { }

/*
static class ConfigManagerJson
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
*/