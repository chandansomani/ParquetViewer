using CsvHelper.Configuration;
using CsvHelper;
using System.Globalization;

namespace CSVProcessor
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var options = CommandLineParser.Parse(args);
            if (options == null) return;

            var csvOperations = new CsvOperations(options);
            csvOperations.ProcessCsvData();
        }
    }
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
                                .Where(n => int.TryParse(n, out _)) // Filter valid integers
                                .Select(int.Parse)
                                .ToList();
                            i++;
                        }
                        break;
                    case "-d":
                    case "--delimiter":
                        if (i + 1 < args.Length)
                        {
                            options.Delimiter = args[i + 1] =="\\t" ? "\t" : args[i + 1];
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
            Console.WriteLine("CsvDuplicateFinder - Find duplicate records in CSV files");
            Console.WriteLine("\nUsage:");
            Console.WriteLine("  CsvDuplicateFinder <file_path> [options]");
            Console.WriteLine("\nOptions:");
            Console.WriteLine("  -f, --fields <field1,field2,...>  Specify fields to check for duplicates (default: all fields)");
            Console.WriteLine("  -c, --columns <col1,col2,...>     Specify column indices to check for duplicates (default: all columns)");
            Console.WriteLine("  -d, --delimiter <char>            Specify CSV delimiter (default: ',')");
            Console.WriteLine("  -h, --header                      Indicates the CSV file has a header row");
            Console.WriteLine("  -v, --verbose                     Show all fields for duplicate records");
            Console.WriteLine("  -l, --limit <number>              Limit number of duplicate groups to display");
            Console.WriteLine("  -s, --stats                       Display statistics about the CSV file");
            Console.WriteLine("  -pf                               Display CSV data (all records)");
            Console.WriteLine("  -pf <number>                      Display first n records from the CSV file");
            Console.WriteLine("  -fd --finddup                     Find Duplicates");
            Console.WriteLine("  --help                            Show this help message");
        }
    }

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

    public class CsvOperations
    {
        private readonly Options _options;
        private List<string> dupColumnHeadings;

        public CsvOperations(Options options)
        {
            _options = options;
        }

        public List<dynamic> ReadCsvData()
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = _options.Delimiter,
                HasHeaderRecord = _options.HasHeader,
                MissingFieldFound = null,
                BadDataFound = context => { /* Handle bad data */ }
            };

            using var reader = new StreamReader(_options.FilePath);
            using var csv = new CsvReader(reader, config);

            return csv.GetRecords<dynamic>().ToList();
        }

        public List<List<string>> FindDuplicates()
        {
            var records = ReadCsvData();
            // Determine the columns to use
            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();
            List<string> columnsToUse = _options.ColumnIndices.Any()
                ? _options.ColumnIndices.Select(i => allColumns[i]).ToList() // Use column indices
                : _options.Fields.Any()
                    ? _options.Fields // Use column names
                    : allColumns; // Use all columns
            dupColumnHeadings = columnsToUse;
            return DuplicateFinder.FindDuplicates(records, columnsToUse);
        }

        public void ProcessCsvData()
        {
            var records = ReadCsvData();
            List<List<string>> duplicates;

            // Print Default Stats
            PrintData.PrintInitialStats(records, _options.Fields, _options.ColumnIndices);

            if (_options.PrintData)
            {
                PrintData.PrintTabularData(records, _options.Fields, _options.ColumnIndices, _options.RowLimit);
            }

            if (_options.FindDuplicates)
            {
                duplicates = FindDuplicates();
                if (duplicates.Count > 0)
                {
                    PrintData.PrintDuplicates(duplicates, dupColumnHeadings);
                    PrintData.PrintStatistics(duplicates);
                }
                else
                {
                    Console.WriteLine("No Duplicates");
                }
            }

            if (_options.ShowStats)
            {
                // Print CSV file statistics
                PrintData.PrintStats(records, _options.Fields, _options.ColumnIndices);
            }

        }
    }

    public static class DuplicateFinder
    {
        public static List<List<string>> FindDuplicates(List<dynamic> records, List<string> columns)
        {
            var duplicates = new List<List<string>>();
            var seen = new Dictionary<string, List<dynamic>>();

            // If no columns are specified, use all fields from the first record
            if (columns == null || columns.Count == 0)
            {
                columns = ((IDictionary<string, object>)records[0]).Keys.ToList();
            }

            foreach (var record in records)
            {
                // Create a key based on the specified columns or all fields
                var key = columns == null
                    ? string.Join("|", ((IDictionary<string, object>)record).Values)
                    : string.Join("|", columns.Select(c => ((IDictionary<string, object>)record)[c]?.ToString() ?? string.Empty));

                // Group records by the key
                if (seen.ContainsKey(key))
                {
                    seen[key].Add(record);
                }
                else
                {
                    seen[key] = new List<dynamic> { record };
                }
            }

            // Extract duplicate groups
            foreach (var group in seen.Values)
            {
                if (group.Count > 1)
                {
                    // Convert each record in the group to a string representation
                    var duplicateGroup = group.Select(r =>
                    {
                        if (columns == null)
                        {
                            // Use all fields if no specific columns are provided
                            return string.Join("|", ((IDictionary<string, object>)r).Values);
                        }
                        else
                        {
                            // Use only the specified columns
                            return string.Join("|", columns.Select(c => ((IDictionary<string, object>)r)[c]?.ToString() ?? string.Empty));
                        }
                    }).ToList();

                    duplicates.Add(duplicateGroup);
                }
            }

            return duplicates;
        }
    }

    public static class PrintData
    {
        // Print duplicates
        public static void PrintDuplicates(List<List<string>> duplicates, List<string> columnsHeading)
        {
            Console.WriteLine("Duplicates found:");
            Console.WriteLine($"Count|{string.Join("|", columnsHeading)}");

            foreach (var group in duplicates)
            {
                if (group.Count > 0)
                {
                    // Assuming all records in the group are the same, so we take the first one
                    var record = group[0];
                    Console.WriteLine($"{group.Count,5}|{record}");
                }
            }
        }

        // Print statistics
        public static void PrintStatistics(List<List<string>> duplicates)
        {
            Console.WriteLine($"Total duplicate groups: {duplicates.Count}");
            Console.WriteLine($"Total duplicate records: {duplicates.Sum(g => g.Count)}");
        }

        // Print CSV data in tabular format
        public static void PrintTabularData(List<dynamic> records, List<string> columns = null, List<int> columnIndices = null, long rowLimit = -1)
        {
            if (records == null || records.Count == 0)
            {
                Console.WriteLine("No data to display.");
                return;
            }

            // Determine the columns to display
            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();
            var columnsToUse = columnIndices?.Any() == true
                ? columnIndices.Select(i => allColumns[i]).ToList() // Use column indices
                : (columns?.Any() == true ? columns : allColumns); // Use column names or all columns


            // Calculate column widths
            var columnWidths = new Dictionary<string, int>();
            foreach (var column in allColumns)
            {
                int maxWidth = column.Length; // Start with the column name length
                foreach (var record in records)
                {
                    var value = ((IDictionary<string, object>)record)[column]?.ToString() ?? string.Empty;
                    if (value.Length > maxWidth)
                    {
                        maxWidth = value.Length;
                    }
                }
                columnWidths[column] = maxWidth + 2; // Add padding
            }

            // Print header
            Console.WriteLine();
            foreach (var column in allColumns)
            {
                Console.Write(column.PadRight(columnWidths[column]));
            }
            Console.WriteLine();

            // Print separator line
            foreach (var column in allColumns)
            {
                Console.Write(new string('-', columnWidths[column]));
            }
            Console.WriteLine();

            // Print rows
            int rowCount = 0;
            foreach (var record in records)
            {
                if (rowLimit > 0 && rowCount >= rowLimit)
                {
                    break;
                }

                foreach (var column in allColumns)
                {
                    var value = ((IDictionary<string, object>)record)[column]?.ToString() ?? string.Empty;
                    Console.Write(value.PadRight(columnWidths[column]));
                }
                Console.WriteLine();
                rowCount++;
            }

            Console.WriteLine();
        }

        // Print CSV file statistics
        public static void PrintStats(List<dynamic> records, List<string> columns = null, List<int> columnIndices = null)
        {
            if (records == null || records.Count == 0)
            {
                Console.WriteLine("No data to generate statistics.");
                return;
            }

            // Determine the columns to analyze
            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();
            var columnsToUse = columnIndices?.Any() == true
                ? columnIndices.Select(i => allColumns[i]).ToList() // Use column indices
                : (columns?.Any() == true ? columns : allColumns); // Use column names or all columns

            // Calculate basic statistics
            int totalRows = records.Count;
            int totalColumns = allColumns.Count;
            
            /*
            var nullCounts = new Dictionary<string, int>();
            var uniqueRows = new HashSet<string>();

            foreach (var column in allColumns)
            {
                nullCounts[column] = 0;
            }

            foreach (var record in records)
            {
                // Count null/empty values
                foreach (var column in allColumns)
                {
                    var value = ((IDictionary<string, object>)record)[column]?.ToString() ?? string.Empty;
                    if (string.IsNullOrEmpty(value))
                    {
                        nullCounts[column]++;
                    }
                }
            }
            */

            // Print statistics
            Console.WriteLine("\nCSV File Statistics:");
            Console.WriteLine("-------------------");
            Console.WriteLine($"Total Rows: {totalRows}");
            Console.WriteLine($"Total Columns: {totalColumns}");
            //Console.WriteLine($"Column Names: {string.Join(", ", allColumns)}");
            // Print column names with index positions in tabular format
            Console.WriteLine("\nColumn Names:");
            Console.WriteLine("Index | Column Name");
            Console.WriteLine("------|------------");
            for (int i = 0; i < allColumns.Count; i++)
            {
                Console.WriteLine($"{i,5} | {allColumns[i]}");
            }

            /*
            Console.WriteLine("\nNull/Empty Value Counts:");
            foreach (var column in allColumns)
            {
                Console.WriteLine($"{column}: {nullCounts[column]}");
            }
            */
        }


        public static void PrintInitialStats(List<dynamic> records, List<string> columns = null, List<int> columnIndices = null)
        {
            if (records == null || records.Count == 0)
            {
                Console.WriteLine("No data to generate statistics.");
                return;
            }

            // Determine the columns to analyze
            var allColumns = ((IDictionary<string, object>)records[0]).Keys.ToList();

            // Calculate basic statistics
            int totalRows = records.Count;
            int totalColumns = allColumns.Count;

            // Print statistics
            Console.WriteLine("\nCSV File Statistics:");
            Console.WriteLine("-------------------");
            Console.WriteLine($"Total Rows: {totalRows} Total Columns: {totalColumns}");
            //Console.WriteLine($"Column Names: {string.Join(", ", allColumns)}");
            // Print column names with index positions in tabular format
            Console.WriteLine("\nColumn Names:");
            Console.WriteLine("Index | Column Name");
            Console.WriteLine("------|------------");
            for (int i = 0; i < allColumns.Count; i++)
            {
                Console.WriteLine($"{i,5} | {allColumns[i]}");
            }

            //Console.WriteLine($"CSV Column Names: {string.Join(", ", allColumns)}");

            var columnsToUse = columnIndices?.Any() == true
                ? columnIndices.Select(i => allColumns[i]).ToList() // Use column indices
                : (columns?.Any() == true ? columns : allColumns); // Use column names or all columns

            //Console.WriteLine($"Selected Column Names: {string.Join(", ", allColumns)}");

            Console.WriteLine("\nSelect CSV Column Names:");
            Console.WriteLine("Index | Column Name");
            Console.WriteLine("------|------------");
            for (int i = 0; i < columnsToUse.Count; i++)
            {
                Console.WriteLine($"{i,5} | {columnsToUse[i]}");
            }


            /*
            Console.WriteLine("\nNull/Empty Value Counts:");
            foreach (var column in allColumns)
            {
                Console.WriteLine($"{column}: {nullCounts[column]}");
            }
            */
        }
    }
}
