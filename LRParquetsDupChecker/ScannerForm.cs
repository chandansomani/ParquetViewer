using LRPayloadValidatorGUI;
using System.Data;
using System.Text;

namespace LRParquetsDupChecker
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
            InitializeComponent();
            InitializeManagers();
            LoadConfigFile();
            options = new Options();
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


        // Business Logic
        private void VerifyFilesWithDataDictionary()
        {
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

        private async Task<string> PerformBusinessLogicAsync(string filePath, CancellationToken token)
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
                List<string> fileColumns;
                if (options.IsCsv)
                {
                    fileColumns = await CsvOperations.GetCSVColumns(options);
                }
                else
                {
                    fileColumns = await ParquetOperations.GetParquetColumns(options);
                }

                if (fileColumns == null || !fileColumns.Any())
                {
                    return "Failed to read columns from file";
                }

                // 1. Validate column names exist in file
                bool allColumnsExist = ValidateColumnExistence(fileColumns, fileConfig);
                validationResults.Add($"Columns Exist: {(allColumnsExist ? "✓" : "✗")}");

                // 2. Validate column sequence
                bool columnSequenceMatch = ValidateColumnSequence(fileColumns, fileConfig);
                validationResults.Add($"Sequence: {(columnSequenceMatch ? "✓" : "✗")}");

                // 3. Get primary key columns
                List<string> primaryKeyColumns = fileConfig.Columns?
                    .Where(c => c.IsPrimaryKey)
                    .Select(c => c.Name)
                    .ToList() ?? new List<string>();

                if (primaryKeyColumns.Any())
                {
                    // 4. Perform duplicate and null checks on PK columns
                    var dataQualityResults = await PerformDataQualityChecksAsync(filePath, primaryKeyColumns, token);
                    validationResults.Add($"Duplicates: {dataQualityResults.DuplicatesFound}");
                    validationResults.Add($"Nulls: {dataQualityResults.NullsFound}");

                    // Update DataGridView with results
                    UpdateDataGridRow(fileName, allColumnsExist, allColumnsExist, columnSequenceMatch,
                                     dataQualityResults.DuplicatesFound, dataQualityResults.NullsFound);
                }
                else
                {
                    validationResults.Add("No PK columns defined");
                    UpdateDataGridRow(fileName, allColumnsExist, allColumnsExist, columnSequenceMatch, 0, 0);
                }

                return string.Join(" | ", validationResults);
            }
            catch (Exception ex)
            {
                return $"Error: {ex.Message}";
            }
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
                    actual.Equals(expected, StringComparison.OrdinalIgnoreCase)));
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

            // Load data for quality checks
            DataTable dataTable;
            if (options.IsCsv)
            {
                dataTable = await Task.Run(() => CsvOperations.LoadCsv(options, primaryKeyColumns));
            }
            else
            {
                dataTable = await Task.Run(() => ParquetOperations.LoadParquet(options, primaryKeyColumns));
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

        private void UpdateDataGridRow(string fileName, bool schemaMatch, bool columnNamesMatch, bool columnSequenceMatch, int duplicatesFound, int nullsFound)
        {
            if (InvokeRequired)
            {
                Invoke(new Action<string, bool, bool, bool, int, int>(
                    UpdateDataGridRow), fileName, schemaMatch, columnNamesMatch, columnSequenceMatch, duplicatesFound, nullsFound);
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
