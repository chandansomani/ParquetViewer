using ParquetViewer.Engine;
using ParquetViewer.Engine.Exceptions;
using ParquetViewer.Engine.Types;
using System.Data;

namespace ParquetDuplicateFinder
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 1)
            {
                PrintUsage();
                return;
            }

            string filePath = args[0];

            if (!File.Exists(filePath) && !Directory.Exists(filePath))
            {
                Console.WriteLine($"Error: The specified file or folder does not exist: {filePath}");
                return;
            }

            List<string> fieldsToCheck = new List<string>();
            bool verbose = false;
            int limit = -1;
            bool showStats = false;
            bool findDuplicates = true;

            // Parse additional arguments
            for (int i = 1; i < args.Length; i++)
            {
                if (args[i] == "-f" || args[i] == "--fields")
                {
                    if (i + 1 < args.Length)
                    {
                        fieldsToCheck.AddRange(args[i + 1].Split(',').Select(f => f.Trim()));
                        i++;
                    }
                }
                else if (args[i] == "-v" || args[i] == "--verbose")
                {
                    verbose = true;
                }
                else if (args[i] == "-l" || args[i] == "--limit")
                {
                    if (i + 1 < args.Length && int.TryParse(args[i + 1], out int parsedLimit))
                    {
                        limit = parsedLimit;
                        i++;
                    }
                }
                else if (args[i] == "-s" || args[i] == "--stats")
                {
                    showStats = true;
                }
                else if (args[i] == "--stats-only")
                {
                    showStats = true;
                    findDuplicates = false;
                }
                else if (args[i] == "-h" || args[i] == "--help")
                {
                    PrintUsage();
                    return;
                }
            }

            try
            {
                Console.WriteLine($"Opening {filePath}...");
                ParquetEngine parquetEngine = await ParquetEngine.OpenFileOrFolderAsync(filePath, CancellationToken.None);

                // Display file statistics if requested
                if (showStats)
                {
                    DisplayParquetStats(parquetEngine, filePath);

                    // If stats-only flag is set, exit after showing stats
                    if (!findDuplicates)
                    {
                        return;
                    }
                }

                // Get all available fields if none specified
                if (fieldsToCheck.Count == 0)
                {
                    // Get the field names from the schema
                    fieldsToCheck = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
                    Console.WriteLine("No fields specified, using all available fields for duplicate checking.");
                }
                else
                {
                    // Validate that all specified fields exist
                    var availableFieldNames = parquetEngine.Schema.Fields.Select(f => f.Name).ToList();
                    var invalidFields = fieldsToCheck.Where(f => !availableFieldNames.Contains(f, StringComparer.OrdinalIgnoreCase)).ToList();

                    if (invalidFields.Any())
                    {
                        Console.WriteLine($"Warning: The following fields do not exist in the Parquet file: {string.Join(", ", invalidFields)}");
                        fieldsToCheck = fieldsToCheck.Where(f => !invalidFields.Contains(f)).ToList();

                        if (fieldsToCheck.Count == 0)
                        {
                            Console.WriteLine("Error: No valid fields to check for duplicates.");
                            return;
                        }
                    }
                }

                Console.WriteLine($"Fields being checked for duplicates: {string.Join(", ", fieldsToCheck)}");
                Console.WriteLine($"Reading data from {filePath}...");

                // Read all rows with specified fields
                var loadResult = await parquetEngine.ReadRowsAsync(
                    fieldsToCheck,
                    0,
                    (int)parquetEngine.RecordCount,
                    CancellationToken.None,
                    null);

                var dataTable = loadResult.Invoke(false);
                Console.WriteLine($"Loaded {dataTable.Rows.Count} rows.");

                // Find duplicates
                FindAndDisplayDuplicates(dataTable, fieldsToCheck, verbose, limit);
            }
            catch (Exception ex)
            {
                if (ex is AllFilesSkippedException afse)
                {
                    Console.WriteLine($"Error: All files were skipped. {afse.Message}");
                }
                else if (ex is SomeFilesSkippedException sfse)
                {
                    Console.WriteLine($"Warning: Some files were skipped. {sfse.Message}");
                }
                else if (ex is FileReadException fre)
                {
                    Console.WriteLine($"Error reading file: {fre.Message}");
                }
                else if (ex is MultipleSchemasFoundException msfe)
                {
                    Console.WriteLine($"Error: Multiple schemas found. {msfe.Message}");
                }
                else
                {
                    Console.WriteLine($"Error: {ex.Message}");
                    if (verbose)
                    {
                        Console.WriteLine(ex.StackTrace);
                    }
                }
            }
        }


        /**
        static void DisplayParquetStats_old(ParquetEngine parquetEngine, string filePath)
        {
            Console.WriteLine("\n===== PARQUET FILE STATISTICS =====");

            // File info
            bool isFolder = Directory.Exists(filePath);
            Console.WriteLine($"Type: {(isFolder ? "Folder" : "File")}");
            Console.WriteLine($"Path: {filePath}");

            // Records info
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
            Console.WriteLine($"Number of Partitions: {parquetEngine.NumberOfPartitions}");
            Console.WriteLine($"Number of Row Groups: {parquetEngine.ThriftMetadata.RowGroups.Count}");

            // Column info
            var fields = parquetEngine.Schema.Fields;
            Console.WriteLine($"\nColumns: {fields.Count}");
            Console.WriteLine("\nColumn Details:");
            Console.WriteLine(new string('-', 80));
            Console.WriteLine($"{"Position",-8} | {"Name",-30} | {"Type",-20} | {"Repetition",-10}");
            Console.WriteLine(new string('-', 80));

            for (int i = 0; i < fields.Count; i++)
            {
                var field = fields[i];
                Console.WriteLine($"{i,-8} | {field.Name,-30} | {field.DataType,-20} | {field.RepetitionType,-10}");
            }

            Console.WriteLine(new string('-', 80));

            // Data types summary
            var dataTypeCounts = fields.GroupBy(f => f.DataType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);

            Console.WriteLine("\nData Type Summary:");
            foreach (var typeCount in dataTypeCounts)
            {
                Console.WriteLine($"  {typeCount.Type,-20}: {typeCount.Count,3} column(s)");
            }

            // Physical storage info (if available)
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
         * 
         */

        static void DisplayParquetStats(ParquetEngine parquetEngine, string filePath)
        {
            Console.WriteLine("\n===== PARQUET FILE STATISTICS =====");
            // File info
            bool isFolder = Directory.Exists(filePath);
            Console.WriteLine($"Type: {(isFolder ? "Folder" : "File")}");
            Console.WriteLine($"Path: {filePath}");
            // Records info
            Console.WriteLine($"Total Records: {parquetEngine.RecordCount:N0}");
            Console.WriteLine($"Number of Partitions: {parquetEngine.NumberOfPartitions}");
            Console.WriteLine($"Number of Row Groups: {parquetEngine.ThriftMetadata.RowGroups.Count}");

            // Column info
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

            // Data types summary
            var schemaTypeCounts = dataFields.GroupBy(f => f.SchemaType)
                .Select(g => new { Type = g.Key, Count = g.Count() })
                .OrderByDescending(x => x.Count);

            Console.WriteLine("\nSchema Type Summary:");
            foreach (var typeCount in schemaTypeCounts)
            {
                Console.WriteLine($"  {typeCount.Type,-20}: {typeCount.Count} column(s)");
            }

            // Physical storage info (if available)
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

        // Helper method to format file size
        static string FormatFileSize(long bytes)
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

        static void FindAndDisplayDuplicates(DataTable dataTable, List<string> fieldsToCheck, bool verbose, int limit)
        {
            Console.WriteLine("Searching for duplicates...");

            // Group by specified fields
            var groups = new Dictionary<string, List<DataRow>>();

            foreach (DataRow row in dataTable.Rows)
            {
                // Create a key based on the values in the fields we're checking
                var keyValues = new List<string>();
                foreach (var field in fieldsToCheck)
                {
                    var value = row[field];
                    keyValues.Add(value == DBNull.Value ? "NULL" : value.ToString());
                }

                string key = string.Join("|", keyValues);

                if (!groups.ContainsKey(key))
                {
                    groups[key] = new List<DataRow>();
                }

                groups[key].Add(row);
            }

            // Filter to only groups with duplicates
            var duplicateGroups = groups.Where(g => g.Value.Count > 1).ToList();

            if (duplicateGroups.Count == 0)
            {
                Console.WriteLine("No duplicates found.");
                return;
            }

            Console.WriteLine($"Found {duplicateGroups.Count} duplicate groups.");

            // Apply limit if specified
            if (limit > 0 && duplicateGroups.Count > limit)
            {
                Console.WriteLine($"Showing first {limit} duplicate groups (use --limit to adjust).");
                duplicateGroups = duplicateGroups.Take(limit).ToList();
            }

            // Display duplicates
            int groupNumber = 1;
            foreach (var group in duplicateGroups)
            {
                Console.WriteLine($"\nDuplicate Group #{groupNumber++} ({group.Value.Count} records):");

                if (verbose)
                {
                    // Display all fields for each duplicate
                    int recordNumber = 1;
                    foreach (var row in group.Value)
                    {
                        Console.WriteLine($"  Record #{recordNumber++}:");
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            var value = row[column];
                            string displayValue;

                            if (value == DBNull.Value)
                            {
                                displayValue = "NULL";
                            }
                            else if (value is ListValue)
                            {
                                // Using ToString() which is implemented in ListValue class
                                displayValue = value.ToString();
                            }
                            else if (value is MapValue)
                            {
                                // Using ToString() which should be implemented in MapValue class
                                displayValue = value.ToString();
                            }
                            else if (value is StructValue)
                            {
                                // Using ToString() which should be implemented in StructValue class
                                displayValue = value.ToString();
                            }
                            else if (value is ByteArrayValue)
                            {
                                // Using ToString() which should be implemented in ByteArrayValue class
                                displayValue = value.ToString();
                            }
                            else
                            {
                                displayValue = value.ToString();
                            }

                            Console.WriteLine($"    {column.ColumnName}: {displayValue}");
                        }
                    }
                }
                else
                {
                    // Display just the key fields for each duplicate
                    foreach (var field in fieldsToCheck)
                    {
                        Console.WriteLine($"  {field}: {group.Value[0][field]}");
                    }
                    Console.WriteLine($"  Count: {group.Value.Count}");
                }
            }

            // Summary
            int totalDuplicates = duplicateGroups.Sum(g => g.Value.Count - 1);
            Console.WriteLine($"\nSummary: Found {totalDuplicates} duplicate records in {duplicateGroups.Count} groups.");
        }

        static void PrintUsage()
        {
            Console.WriteLine("ParquetDuplicateFinder - Find duplicate records in Parquet files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  ParquetDuplicateFinder <file_or_folder_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Specify fields to check for duplicates (default: all fields)");
            Console.WriteLine("  -v, --verbose                     Show all fields for duplicate records");
            Console.WriteLine("  -l, --limit <number>              Limit number of duplicate groups to display");
            Console.WriteLine("  -s, --stats                       Display statistics about the Parquet file");
            Console.WriteLine("  --stats-only                      Display only statistics (no duplicate checking)");
            Console.WriteLine("  -h, --help                        Show this help message");
            Console.WriteLine("\nExamples:");
            Console.WriteLine("  ParquetDuplicateFinder data.parquet");
            Console.WriteLine("  ParquetDuplicateFinder data.parquet -f id,name,email");
            Console.WriteLine("  ParquetDuplicateFinder data_folder -v -l 10");
            Console.WriteLine("  ParquetDuplicateFinder data.parquet --stats");
            Console.WriteLine("  ParquetDuplicateFinder data.parquet --stats-only");
        }
    }
}