using System.Data;
using System.Text;

namespace LRPayloadValidatorGUI
{
    public partial class ScannerForm : Form
    {
        private Options options;
        private ConfigFile? pkMasterInfo;
        private CancellationTokenSource cts;

        private DataGridManager dataGridManager;
        private TaskQueueManager taskQueueManager;

        public ScannerForm()
        {
            options = new Options();
            InitializeComponent();
            InitializeManagers();
            LoadConfigFile();
        }

        private void InitializeManagers()
        {
            dataGridManager = new DataGridManager(dataGridView1);
            taskQueueManager = new TaskQueueManager(listViewTasks, progressBarOverall, this);

            startToolStripMenuItem.Enabled = true;
            cancelToolStripMenuItem.Enabled = false;
        }

        private void LoadConfigFile()
        {
            if (File.Exists(PKConfigManager.DefaultConfigFile))
            {
                options.ConfigFilePath = PKConfigManager.DefaultConfigFile;
                pkMasterInfo = PKConfigManager.LoadFromExcel(options.ConfigFilePath);
            }
            else
            {
                pkMasterInfo = new ConfigFile { ParquetFiles = new Dictionary<string, ParquetFileConfig>() };
            }
        }

        // Event Handlers
        private void openFolderToolStripMenuItem_Click(object sender, EventArgs e)
        {
            string folderPath;
            using (var dlg = new FolderBrowserDialog())
            {
                if (dlg.ShowDialog() == DialogResult.OK)
                    folderPath = dlg.SelectedPath;
                else
                    return;
            }

            this.Text = $"Scanner Form - {folderPath}";
            dataGridManager.LoadFilesFromFolder(folderPath);
        }

        private void selectAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            dataGridManager.SelectAll(true);
        }

        private void deSelectAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            dataGridManager.SelectAll(false);
        }

        private void loadDataDictionaryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            LoadConfigFile();
            toolStripStatusLabel1.Text = $"Loaded - Data Dictionary, Parquet Files Count : {pkMasterInfo?.ParquetFiles.Count}";
        }

        private void verifyFilesWithDDToolStripMenuItem_Click(object sender, EventArgs e)
        {
            VerifyFilesWithDataDictionary();
        }
        
        private void addToQueueToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var selectedFiles = dataGridManager.GetSelectedFiles();
            taskQueueManager.AddTasks(selectedFiles);
            toolStripStatusLabel1.Text = "Selected files added to the task queue.";
        }

        private async void btnStart_Click(object sender, EventArgs e)
        {
            startToolStripMenuItem.Enabled = false;
            cancelToolStripMenuItem.Enabled = true;
            cts = new CancellationTokenSource();
            taskQueueManager.ResetProgress();

            try
            {
                await ProcessTaskQueueAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                MessageBox.Show("Processing canceled.");
            }
            finally
            {
                startToolStripMenuItem.Enabled = true;
                cancelToolStripMenuItem.Enabled = false;
            }
        }

        private void btnCancel_Click(object sender, EventArgs e)
        {
            cts?.Cancel();
        }

        // Business Logic
        private async void VerifyFilesWithDataDictionary()
        {
            cts = new CancellationTokenSource();
            for (int i = 0; i < dataGridView1.Rows.Count; i++)
            {
                DataGridViewRow row = dataGridView1.Rows[i];
                string fileName = row.Cells[ArtifactColumns.FileName].Value?.ToString();

                bool isMatch = pkMasterInfo.ParquetFiles.Keys
                    .Any(k => k.Equals(fileName, StringComparison.OrdinalIgnoreCase));

                row.DefaultCellStyle.BackColor = isMatch ?
                    Color.FromArgb(240, 255, 240) :
                    Color.FromArgb(245, 200, 200);

                row.Cells[ArtifactColumns.Select].Value = isMatch;
            }
            await Task.Run(async () => await ProcessValidationAsync(cts.Token));
        }

        private async Task ProcessTaskQueueAsync(CancellationToken token)
        {
            int maxDegreeOfParallelism = 2;
            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var task in taskQueueManager.GetPendingTasks())
            {
                await semaphore.WaitAsync(token);
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        taskQueueManager.UpdateTaskStatus(task, "Processing", "");
                        string result = await PerformBusinessLogicAsync(task.FullPath, token);
                        taskQueueManager.UpdateTaskStatus(task, "Completed", result);
                    }
                    catch (OperationCanceledException)
                    {
                        taskQueueManager.UpdateTaskStatus(task, "Canceled", "");
                    }
                    catch (Exception ex)
                    {
                        taskQueueManager.UpdateTaskStatus(task, "Failed", ex.Message);
                    }
                    finally
                    {
                        semaphore.Release();
                        taskQueueManager.UpdateProgress();
                    }
                }, token));
            }

            await Task.WhenAll(tasks);
        }

        private async Task ProcessValidationAsync(CancellationToken token)
        {
            int maxDegreeOfParallelism = 2;
            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var task in taskQueueManager.GetValidationPendingTasks())
            {
                await semaphore.WaitAsync(token);
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        taskQueueManager.UpdateValidationStatus(task, false, "");
                        string result = await PerformValidationAsync(task.FullPath, token);
                        taskQueueManager.UpdateValidationStatus(task, true, result);
                    }
                    catch (OperationCanceledException)
                    {
                        taskQueueManager.UpdateValidationStatus(task, false, "");
                    }
                    catch (Exception ex)
                    {
                        taskQueueManager.UpdateValidationStatus(task, false, ex.Message);
                    }
                    finally
                    {
                        semaphore.Release();
                        taskQueueManager.UpdateProgress();
                    }
                }, token));
            }

            await Task.WhenAll(tasks);
        }

        private async Task<string> PerformValidationAsync(string filePath, CancellationToken token)
        {
            try
            {
                string fileName = Path.GetFileName(filePath);

                // Update options for the file type
                UpdateOptionsForFile(options, fileName);

                // Get file configuration from master info (case-insensitive match)
                var fileConfig = pkMasterInfo?.ParquetFiles
                    .FirstOrDefault(kv => kv.Key.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                    .Value;

                if (fileConfig == null)
                {
                    return "No config found in Data Dictionary";
                }

                // Read the file and perform validations
                List<string> validationResults = new List<string>();

                // Read columns from the file
                //List<string> fileColumns;
                ParquetInfoResult parquetInfoResult = new ();                
                if (options.IsCsv)
                {
                    parquetInfoResult.Columns = await CsvOperations.GetCSVColumns(options);
                }
                else
                {
                    var opt = new Options() { FilePath = filePath };
                    //fileColumns = await ParquetOperations.GetParquetColumns(opt);
                    parquetInfoResult = await ParquetOperations.GetParquetDetailsAsync(opt);
                    if(parquetInfoResult == null)
                    {
                        return "Failed to read columns from file";
                    }
                    else
                    {
                        if (parquetInfoResult.Columns == null || !parquetInfoResult.Columns.Any())
                        {
                            return "Failed to read columns from file";
                        }
                    }
                }

                // 1. Validate column names exist in file
                bool allColumnsExist = ValidateColumnExistence(parquetInfoResult.Columns, fileConfig);
                validationResults.Add($"Columns Exist: {(allColumnsExist ? "✓" : "✗")}");

                // 2. Validate column sequence
                bool columnSequenceMatch = ValidateColumnSequence(parquetInfoResult.Columns, fileConfig);
                validationResults.Add($"Sequence: {(columnSequenceMatch ? "✓" : "✗")}");


                List<string> primaryKeyColumns = fileConfig.Columns?
                    .Where(c => c.IsPrimaryKey)
                    .Select(c => c.Name)
                    .ToList() ?? new List<string>();

                if (primaryKeyColumns.Any())
                {
                    validationResults.Add($"PK columns Count {primaryKeyColumns.Count}");
                }
                else
                {
                    validationResults.Add("No PK columns defined");
                }

                {
                    var extraColumns = GetExtraColumns(parquetInfoResult.Columns, fileConfig);
                    var analysisResults = AnalyzeColumnDifferences(parquetInfoResult.Columns, fileConfig);
                    int matchedCount = parquetInfoResult.Columns.Count - analysisResults.Count - extraColumns.Count;
                    int missingCount = analysisResults.Count;
                    int extraCount = extraColumns.Count;

                    string summary =
                                $"• Matched: {matchedCount,2:D2} " +
                                $"• Missing: {missingCount,2:D2} " +
                                $"• Extra  : {extraCount,2:D2} ";
                    validationResults.Add($"Columns {summary}");

                    foreach (var result in analysisResults)
                    {
                        validationResults.Add(
                            $"Expected: '{result.Expected}', Closest Actual: '{result.ClosestActual ?? "N/A"}', " +
                            $"Type: {result.DifferenceType}, Levenshtein: {result.LevenshteinDistance}");
                    }

                    if (extraColumns.Any())
                    {
                        validationResults.Add($"Extra Columns: {string.Join(", ", extraColumns)}");
                    }
                }
                UpdateValidationDataGridRow(fileName, allColumnsExist, allColumnsExist, columnSequenceMatch);
                validationResults.Add($"Records: {parquetInfoResult.RecordCount}");
                return string.Join(" | ", validationResults);
            }
            catch (Exception ex)
            {
                return $"Error: {ex.Message}";
            }
        }

        private async Task<string> PerformBusinessLogicAsync(string filePath, CancellationToken token)
        {
            try
            {
                string fileName = Path.GetFileName(filePath);
                
                var fileConfig = pkMasterInfo?.ParquetFiles
                    .FirstOrDefault(kv => kv.Key.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                    .Value;

                if (fileConfig == null)
                {
                    return "No config found in Data Dictionary";
                }

                List<string> validationResults = new List<string>();
                List<string> primaryKeyColumns = fileConfig.Columns?
                    .Where(c => c.IsPrimaryKey)
                    .Select(c => c.Name)
                    .ToList() ?? new List<string>();

                if (primaryKeyColumns.Any())
                {
                    var dataQualityResults = await PerformDataQualityChecksAsync(filePath, primaryKeyColumns, token);
                    validationResults.Add($"Duplicates: {dataQualityResults.DuplicatesFound}");
                    validationResults.Add($"Nulls: {dataQualityResults.NullsFound}");

                    // Update DataGridView with results
                    UpdateQualityResultsDataGridRow(fileName, dataQualityResults.DuplicatesFound, dataQualityResults.NullsFound);
                }
                else
                {
                    validationResults.Add("No PK columns defined");
                    UpdateQualityResultsDataGridRow(fileName, 0, 0);
                }

                return string.Join(" | ", validationResults);
            }
            catch (Exception ex)
            {
                return $"Error: {ex.Message}";
            }
        }

        public static List<string> GetExtraColumns(List<string> fileColumns, ParquetFileConfig fileConfig)
        {
            var expectedColumns = fileConfig.Columns?.Select(c => c.Name).ToList() ?? new List<string>();
            return fileColumns.Where(actual =>
                !expectedColumns.Any(expected => actual.Equals(expected, StringComparison.Ordinal))
            ).ToList();
        }

        public static List<ColumnAnalysisResult> AnalyzeColumnDifferences(List<string> fileColumns, ParquetFileConfig fileConfig)
        {
            var expectedColumns = fileConfig.Columns?.Select(c => c.Name).ToList() ?? new List<string>();
            var results = new List<ColumnAnalysisResult>();

            foreach (var expected in expectedColumns)
            {
                if (fileColumns.Any(actual => actual.Equals(expected, StringComparison.Ordinal)))
                    continue; // Exact match found

                // Find closest actual column
                string closest = null;
                int minDistance = int.MaxValue;
                foreach (var actual in fileColumns)
                {
                    int distance = LevenshteinDistance(expected, actual);
                    if (distance < minDistance)
                    {
                        minDistance = distance;
                        closest = actual;
                    }
                }

                string diffType = "Missing";
                if (closest != null)
                {
                    if (expected.Equals(closest, StringComparison.OrdinalIgnoreCase))
                        diffType = "Case difference";
                    else if (expected.Trim().Equals(closest.Trim(), StringComparison.Ordinal))
                        diffType = "Whitespace difference";
                    else if (minDistance <= 2)
                        diffType = $"Possible typo (distance {minDistance})";
                    else
                        diffType = $"Different (distance {minDistance})";
                }

                results.Add(new ColumnAnalysisResult
                {
                    Expected = expected,
                    ClosestActual = closest,
                    DifferenceType = diffType,
                    LevenshteinDistance = minDistance
                });
            }

            return results;
        }

        public static int LevenshteinDistance(string s, string t)
        {
            if (string.IsNullOrEmpty(s)) return t?.Length ?? 0;
            if (string.IsNullOrEmpty(t)) return s.Length;

            var d = new int[s.Length + 1, t.Length + 1];

            for (int i = 0; i <= s.Length; i++) d[i, 0] = i;
            for (int j = 0; j <= t.Length; j++) d[0, j] = j;

            for (int i = 1; i <= s.Length; i++)
            {
                for (int j = 1; j <= t.Length; j++)
                {
                    int cost = (s[i - 1] == t[j - 1]) ? 0 : 1;
                    d[i, j] = Math.Min(
                        Math.Min(d[i - 1, j] + 1, d[i, j - 1] + 1),
                        d[i - 1, j - 1] + cost
                    );
                }
            }
            return d[s.Length, t.Length];
        }

        private void UpdateOptionsForFile(Options options, string fileName)
        {
            if (fileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                fileName.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase))
            {
                options.IsCsv = true;
                options.Delimiter = options.Delimiter != '\0' ? options.Delimiter : ',';
                options.HasHeader = true;
            }
            else if (fileName.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                     fileName.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase))
            {
                options.IsCsv = false;
            }
        }

        private bool ValidateColumnExistence(List<string> fileColumns, ParquetFileConfig fileConfig)
        {
            var expectedColumns = fileConfig.Columns?.Select(c => c.Name).ToList() ?? new List<string>();

            return expectedColumns.All(expected =>
                fileColumns.Any(actual =>
                    actual.Equals(expected, StringComparison.Ordinal)));
        }

        private bool ValidateColumnSequence(List<string> fileColumns, ParquetFileConfig fileConfig)
        {
            var expectedColumns = fileConfig.Columns?.Select(c => c.Name).ToList() ?? new List<string>();

            // Check if columns are in the same order (case-insensitive)
            for (int i = 0; i < Math.Min(expectedColumns.Count, fileColumns.Count); i++)
            {
                if (!fileColumns[i].Equals(expectedColumns[i], StringComparison.OrdinalIgnoreCase))
                    return false;
            }

            return true;
        }

        private async Task<DataQualityResults> PerformDataQualityChecksAsync(string filePath, List<string> primaryKeyColumns, CancellationToken token)
        {
            var results = new DataQualityResults();

            if (!primaryKeyColumns.Any())
                return results;
            var opt = new Options() { FilePath = filePath };
            // Load data for quality checks
            DataTable dataTable;
            if (options.IsCsv)
            {
                dataTable = await Task.Run(() => CsvOperations.LoadCsv(opt, primaryKeyColumns));
            }
            else
            {
                dataTable = await Task.Run(() => ParquetOperations.LoadParquet(opt, primaryKeyColumns));
            }

            // Check for duplicates
            results.DuplicatesFound = CheckForDuplicates(dataTable, primaryKeyColumns);

            // Check for nulls in PK columns
            results.NullsFound = CheckForNulls(dataTable, primaryKeyColumns);

            return results;
        }

        private int CheckForDuplicates(DataTable data, List<string> pkColumns)
        {
            if (pkColumns.Count == 0) return 0;

            var duplicateGroups = data.AsEnumerable()
                .GroupBy(row => CreateCompositeKey(row, pkColumns))
                .Where(g => g.Count() > 1);

            return duplicateGroups.Sum(g => g.Count() - 1); // Count duplicates (excluding first occurrence)
        }

        private int CheckForNulls(DataTable data, List<string> pkColumns)
        {
            return data.AsEnumerable()
                .Count(row => pkColumns.Any(col => row[col] == null || row[col] == DBNull.Value));
        }

        private string CreateCompositeKey(DataRow row, List<string> columns)
        {
            var keyBuilder = new StringBuilder();
            foreach (var column in columns)
            {
                keyBuilder.Append(row[column]?.ToString() ?? "NULL").Append("|");
            }
            return keyBuilder.ToString();
        }

        private void UpdateValidationDataGridRow(string fileName, bool schemaMatch, bool columnNamesMatch, bool columnSequenceMatch)
        {
            if (InvokeRequired)
            {
                Invoke(new Action<string, bool, bool, bool>(
                    UpdateValidationDataGridRow), fileName, schemaMatch, columnNamesMatch, columnSequenceMatch);
                return;
            }

            // Find the row with matching filename
            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                if (row.IsNewRow) continue;

                var rowFileName = row.Cells[ArtifactColumns.FileName].Value?.ToString();
                if (rowFileName != null && rowFileName.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                {
                    row.Cells[ArtifactColumns.SchemaMatch].Value = schemaMatch;
                    row.Cells[ArtifactColumns.ColumnNamesMatch].Value = columnNamesMatch;
                    row.Cells[ArtifactColumns.ColumnSequenceMatch].Value = columnSequenceMatch;
                    if(!(schemaMatch && columnNamesMatch && columnSequenceMatch))
                    {
                        row.DefaultCellStyle.BackColor = Color.FromArgb(245, 200, 200);
                    }
                    break;
                }
            }
        }

        private void UpdateQualityResultsDataGridRow(string fileName, int duplicatesFound, int nullsFound)
        {
            if (InvokeRequired)
            {
                Invoke(new Action<string, int, int>(
                    UpdateQualityResultsDataGridRow), fileName, duplicatesFound, nullsFound);
                return;
            }

            // Find the row with matching filename
            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                if (row.IsNewRow) continue;

                var rowFileName = row.Cells[ArtifactColumns.FileName].Value?.ToString();
                if (rowFileName != null && rowFileName.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                {
                    row.Cells[ArtifactColumns.DuplicateCheckDone].Value = true;
                    row.Cells[ArtifactColumns.DuplicatesFound].Value = duplicatesFound;
                    row.Cells[ArtifactColumns.NullsFound].Value = nullsFound;
                    row.Cells[ArtifactColumns.Status].Value = "Completed";
                    break;
                }
            }
        }

    }
}
