using CsvHelper.Configuration;
using CsvHelper;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using DocumentFormat.OpenXml;
using ExcelDataReader;
using ParquetViewer.Engine;
using System.Data;
using System.Globalization;
using System.IO.Compression;

namespace LRPayloadValidatorGUI
{
    public class LREngine
    {
    }
    class Options
    {
        public string FilePath { get; set; }
        public bool Verbose { get; set; }
        public bool IsCsv { get; set; }
        public bool HasHeader { get; set; }
        public Dictionary<string, List<string>> PrimaryKeyColumns { get; set; }
        public string ConfigFilePath { get; set; }
        public string LogFolder { get; set; } = "Logs";
        public int Limit { get; set; } = -1;
        public char Delimiter { get; set; }
    }

    static class ParquetOperations
    {
        public static async Task<DataTable> LoadParquet(Options options, List<string> columnsToUse)
        {
            var parquetEngine = await ParquetEngine.OpenFileAsync(options.FilePath, CancellationToken.None);
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

        public static async Task<List<string>> GetParquetColumns(Options options)
        {
            try
            {
                var parquetEngine = await ParquetEngine.OpenFileAsync(options.FilePath, CancellationToken.None);
                return parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading Parquet columns from '{options.FilePath}': {ex.Message}");
                return null;
            }
        }

        public static async Task<ParquetInfoResult> GetParquetDetailsAsync(Options options)
        {
            try
            {
                var parquetEngine = await ParquetEngine.OpenFileAsync(options.FilePath, CancellationToken.None);

                var columns = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
                var recordCount = parquetEngine.RecordCount;

                return new ParquetInfoResult
                {
                    Columns = columns,
                    RecordCount = recordCount
                };
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"Error reading Parquet columns from '{options.FilePath}': {ex.Message}");
                return null;
            }
        }

    }

    public class ParquetInfoResult
    {
        public List<string> Columns { get; set; } = new List<string>();
        public long RecordCount { get; set; } = 0;
    }

    public static class PKConfigManager
    {
        public const string DefaultConfigFile = "pklist.xlsx";

        static PKConfigManager()
        {
            // Register code pages encoding to support Windows-1252
            System.Text.Encoding.RegisterProvider(System.Text.CodePagesEncodingProvider.Instance);
        }

        //Write Columns Info from parquet file to excel file
        static void ReconfigColumnsFromFile(Options options)
        {
            if (string.IsNullOrEmpty(options.FilePath))
            {
                throw new ArgumentException("File path must be provided.");
            }

            string configFilePath = DefaultConfigFile;
            string fileName = Path.GetFileName(options.FilePath);

            if (options.Verbose) Console.WriteLine($"Reading columns from '{options.FilePath}' to update '{configFilePath}'...");

            // Get columns from the file
            List<string> columns = ParquetOperations.GetParquetColumns(options).Result;

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
                var config = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };
                return config;
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
                var config = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };
                //TODO - Report File Load Status to UI User 
                return config;
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

        /**
         * LoadPrimaryKeyColumns Returns all Parquet Column PK Information 
         */
        public static Dictionary<string, List<string>> LoadPrimaryKeyColumns(string configFilePath, string targetFilePath)
        {
            try
            {
                var config = LoadFromExcel(configFilePath);

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
                if (options.Verbose) Console.WriteLine(ex.Message);
                Program.Log(ex.Message);
                throw;
            }


            if (options.Verbose) Console.WriteLine($"Loaded {dataTable.Rows.Count} rows from CSV.");
            return dataTable;
        }

        public static async Task<List<string>> GetCSVColumns(Options options)
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
                HasHeaderRecord = true
            });

            if (csv.Read() && options.HasHeader)
            {
                csv.ReadHeader();
                var columns = csv.HeaderRecord;
                if (columns != null)
                    return columns.ToList();
            }
            return new List<string>();
        }
    }
}
