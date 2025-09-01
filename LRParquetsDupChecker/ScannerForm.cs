using LRPayloadValidatorGUI;
using System.Data;

namespace LRParquetsDupChecker
{
    public partial class ScannerForm : Form
    {
        Options options;
        DataTable artifactTable;
        List<ColumnMetadata> columns;
        ConfigFile? pkMasterInfo;

        private List<FileProcessingTask> taskQueue = new List<FileProcessingTask>();
        private CancellationTokenSource cts;
        private int tasksCompleted = 0;

        public ScannerForm()
        {
            InitializeComponent();
            InitializeTaskQueueUI();
            artifactTable = new DataTable();

            options = new Options();

            columns = new List<ColumnMetadata>
            {
                new(ArtifactColumns.SRNO, typeof(int), ArtifactColumns.SRNO, true),
                new(ArtifactColumns.Select, typeof(bool), ArtifactColumns.Select, true),
                new(ArtifactColumns.FileName, typeof(string), ArtifactColumns.FileName, false),
                new(ArtifactColumns.Status, typeof(string), ArtifactColumns.Status, false),
                new(ArtifactColumns.FullPath, typeof(string), ArtifactColumns.FullPath, false, false), // hidden
                new(ArtifactColumns.SchemaMatch, typeof(bool), ArtifactColumns.SchemaMatch, false),
                new(ArtifactColumns.ColumnNamesMatch, typeof(bool), ArtifactColumns.ColumnNamesMatch, false),
                new(ArtifactColumns.ColumnSequenceMatch, typeof(bool), ArtifactColumns.ColumnSequenceMatch, false),
                new(ArtifactColumns.DuplicateCheckDone, typeof(bool), ArtifactColumns.DuplicateCheckDone, false),
                new(ArtifactColumns.DuplicatesFound, typeof(int), ArtifactColumns.DuplicatesFound, false),
                new(ArtifactColumns.NullsFound, typeof(int), ArtifactColumns.NullsFound, false),
            };

            dataGridView1.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells;
            dataGridView1.DataSource = artifactTable;

            foreach (var col in columns)
            {
                artifactTable.Columns.Add(col.Name, col.DataType);
            }

            dataGridView1.Columns[ArtifactColumns.SRNO].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            dataGridView1.Columns[ArtifactColumns.DuplicatesFound].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            dataGridView1.Columns[ArtifactColumns.NullsFound].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            dataGridView1.Columns[ArtifactColumns.Status].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleCenter;



            // Configure columns dynamically
            foreach (var col in columns)
            {
                var gridCol = dataGridView1.Columns[col.Name];
                if (gridCol != null)
                {
                    gridCol.HeaderText = col.DisplayName;
                    gridCol.ReadOnly = !col.IsEditable;
                    gridCol.Visible = col.Visible;
                }
            }


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

        private void InitializeTaskQueueUI()
        {
            // Setup ListView
            listViewTasks.View = View.Details;
            listViewTasks.FullRowSelect = true;
            listViewTasks.Columns.Add("ID", 50);
            listViewTasks.Columns.Add("File Name", 200);
            listViewTasks.Columns.Add("Status", 100);
            listViewTasks.Columns.Add("Result", 200);

            progressBarOverall.Minimum = 0;
            progressBarOverall.Value = 0;
            progressBarOverall.Step = 1;
            startToolStripMenuItem.Enabled = true;
            cancelToolStripMenuItem.Enabled = false;
        }

        public void OpenFolder()
        {
            string folderPath;
            using (var dlg = new FolderBrowserDialog())
            {
                if (dlg.ShowDialog() == DialogResult.OK)
                    folderPath = dlg.SelectedPath;
                else
                    folderPath = string.Empty;
            }

            IEnumerable<string> filesToProcess;
            if (folderPath == string.Empty)
            {
                /** Check & Implement Action Operation **/
                filesToProcess = new[] { options.FilePath };
                artifactTable.Rows.Clear();
            }
            else
            {
                this.Text = this.Text + " - " + folderPath;
                /** Load all valid parquet Filenames of current dir to fileList UIComponent **/
                filesToProcess = Directory.EnumerateFiles(folderPath, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase));

                artifactTable.Rows.Clear();
                int i = 1; // For SRNo
                foreach (var file in filesToProcess)
                {
                    var rowValues = new object[columns.Count];

                    for (int colIdx = 0; colIdx < columns.Count; colIdx++)
                    {
                        var col = columns[colIdx];

                        switch (col.Name)
                        {
                            case ArtifactColumns.SRNO:
                                rowValues[colIdx] = i++;
                                break;
                            case ArtifactColumns.Select:
                                rowValues[colIdx] = false;
                                break;
                            case ArtifactColumns.FileName:
                                rowValues[colIdx] = Path.GetFileName(file);
                                break;
                            case ArtifactColumns.FullPath:
                                rowValues[colIdx] = file;
                                break;
                            case ArtifactColumns.SchemaMatch:
                            case ArtifactColumns.ColumnNamesMatch:
                            case ArtifactColumns.ColumnSequenceMatch:
                            case ArtifactColumns.DuplicateCheckDone:
                                rowValues[colIdx] = false;
                                break;
                            case ArtifactColumns.DuplicatesFound:
                            case ArtifactColumns.NullsFound:
                                rowValues[colIdx] = 0;
                                break;
                            case ArtifactColumns.Status:
                                rowValues[colIdx] = "Pending";
                                break;
                            default:
                                rowValues[colIdx] = DBNull.Value; // For any unexpected columns
                                break;
                        }
                    }
                    artifactTable.Rows.Add(rowValues);
                }
            }
        }

        private void openFolderToolStripMenuItem_Click(object sender, EventArgs e)
        {
            OpenFolder();
        }

        private async void openParquetToolStripMenuItem_Click(object sender, EventArgs e)
        {
            
        }

        /*
        public async Task ProcessSelectedArtifactsAsync(int maxDegreeOfParallelism = 2)
        {
            string selectColumn = ArtifactColumns.Select;
            string statusColumn = ArtifactColumns.Status;

            var selectedRows = new List<DataGridViewRow>();
            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                if (row.IsNewRow || !row.Visible)
                    continue;
                var selectCell = row.Cells[selectColumn];
                if (selectCell.Value != null && bool.TryParse(selectCell.Value.ToString(), out bool isSelected) && isSelected)
                {
                    selectedRows.Add(row);
                    row.Cells[selectColumn].Value = false;
                }
            }

            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var row in selectedRows)
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        string? fileName = row.Cells[ArtifactColumns.FullPath].Value?.ToString();
                        string outcome = !string.IsNullOrEmpty(fileName)
                            ? await PerformBusinessLogicAsync(fileName)
                            : "Can't Operate File";

                        dataGridView1.Invoke(new Action(() =>
                        {
                            row.Cells[statusColumn].Value = outcome;
                        }));
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        private async Task<string> PerformBusinessLogicAsync(string fileName)
        {
            // Simulate async work
            await Task.Delay(1000);

            string response;
            if (System.IO.File.Exists(fileName)) {
                    AddTasksToQueue(new List<string> { fileName });
                    response = "Added";
            }
            else
                response = "File Not Found";
            return response;

        }
        */

        public void AddSelectedArtifactsToQueue()
        {
            string selectColumn = ArtifactColumns.Select;
            var selectedFiles = new List<string>();

            foreach (DataGridViewRow row in dataGridView1.Rows)
            {
                if (row.IsNewRow || !row.Visible)
                    continue;

                var selectCell = row.Cells[selectColumn];
                if (selectCell.Value != null && bool.TryParse(selectCell.Value.ToString(), out bool isSelected) && isSelected)
                {
                    string filePath = row.Cells[ArtifactColumns.FullPath].Value?.ToString();
                    if (!string.IsNullOrEmpty(filePath))
                        selectedFiles.Add(filePath);

                    row.Cells[selectColumn].Value = false; // Unselect after adding
                }
            }

            AddTasksToQueue(selectedFiles);
        }



        private void selectAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int selectColIndex = artifactTable.Columns.IndexOf("Select");

            // Loop through each row and set "Select" to true
            foreach (DataRow row in artifactTable.Rows)
            {
                row["Select"] = true;
            }
        }

        private void deSelectAllToolStripMenuItem_Click(object sender, EventArgs e)
        {
            int selectColIndex = artifactTable.Columns.IndexOf("Select");

            // Loop through each row and set "Select" to true
            foreach (DataRow row in artifactTable.Rows)
            {
                row["Select"] = false;
            }
        }

        private void loadDataDictionaryToolStripMenuItem_Click(object sender, EventArgs e)
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
            toolStripStatusLabel1.Text = $"Loaded - Data Dictionary, Parquet Files Count : {pkMasterInfo.ParquetFiles.Count}";
        }

        private void verifyFilesWithDDToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //toolStripStatusLabel1.Text = $"Data Dictionary File Count : {pkMasterInfo.ParquetFiles.Count}";

            // Loop through each row in artifactTable
            for (int i = 0; i < artifactTable.Rows.Count; i++)
            {
                DataRow row = artifactTable.Rows[i];
                string fileName = row[ArtifactColumns.FileName]?.ToString();

                // Check if fileName exists in pkMasterInfo.ParquetFiles
                //if (pkMasterInfo.ParquetFiles.ContainsKey(fileName))
                if (pkMasterInfo.ParquetFiles.Keys.Any(k => k.Equals(fileName, StringComparison.OrdinalIgnoreCase)))
                {
                    // Update DataGridView row background color to light green
                    dataGridView1.Rows[i].DefaultCellStyle.BackColor = Color.FromArgb(240, 255, 240); // Light green tint
                    row[ArtifactColumns.Select] = true;
                }
                else
                {
                    // Optionally reset color if not matched
                    dataGridView1.Rows[i].DefaultCellStyle.BackColor = Color.FromArgb(245, 200, 200);
                    row[ArtifactColumns.Select] = false;
                }
            }
        }


        private void AddTasksToQueue(List<string> files)
        {
            int nextId = taskQueue.Count + 1;

            foreach (var file in files)
            {
                if (System.IO.File.Exists(file))
                {
                    string fileName = Path.GetFileName(file);
                    var task = new FileProcessingTask
                    {
                        Id = nextId++,
                        FileName = fileName,
                        FullPath = file,
                        Status = "Pending",
                        Result = ""
                    };
                    taskQueue.Add(task);

                    var item = new ListViewItem(
                        new[]
                        {
                            task.Id.ToString(),
                            task.FileName,
                            task.Status,
                            task.Result
                        })
                    {
                        Tag = task
                    };
                    listViewTasks.Items.Add(item);
                }
            }

            progressBarOverall.Maximum = taskQueue.Count;
            progressBarOverall.Value = tasksCompleted;
        }

        private async void btnStart_Click(object sender, EventArgs e)
        {
            startToolStripMenuItem.Enabled = false;
            cancelToolStripMenuItem.Enabled = true;
            cts = new CancellationTokenSource();
            tasksCompleted = 0;
            progressBarOverall.Value = 0;

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
            int maxDegreeOfParallelism = 2; // Adjust as needed
            var semaphore = new SemaphoreSlim(maxDegreeOfParallelism);
            var tasks = new List<Task>();

            foreach (var task in taskQueue.Where(t => t.Status == "Pending"))
            {
                await semaphore.WaitAsync(token);
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        UpdateTaskStatus(task, "Processing", "");
                        string result = await PerformBusinessLogicAsync(task.FullPath, token);
                        UpdateTaskStatus(task, "Completed", result);
                    }
                    catch (OperationCanceledException)
                    {
                        UpdateTaskStatus(task, "Canceled", "");
                    }
                    catch (Exception ex)
                    {
                        UpdateTaskStatus(task, "Failed", ex.Message);
                    }
                    finally
                    {
                        semaphore.Release();
                        UpdateProgressBar();
                    }
                }, token));
            }

            await Task.WhenAll(tasks);
        }

        private void UpdateTaskStatus(FileProcessingTask task, string status, string result)
        {
            if (InvokeRequired)
            {
                Invoke(new Action(() => UpdateTaskStatus(task, status, result)));
                return;
            }
            task.Status = status;
            task.Result = result;
            foreach (ListViewItem item in listViewTasks.Items)
            {
                if (item.Tag == task)
                {
                    item.SubItems[2].Text = status;
                    item.SubItems[3].Text = result;
                    break;
                }
            }
        }

        private void UpdateProgressBar()
        {
            if (InvokeRequired)
            {
                Invoke(new Action(UpdateProgressBar));
                return;
            }
            tasksCompleted++;
            if (tasksCompleted <= progressBarOverall.Maximum) progressBarOverall.Value = tasksCompleted;
        }

        // Simulated async business logic (replace with your real logic)
        private async Task<string> PerformBusinessLogicAsync(string filePath, CancellationToken token)
        {
            await Task.Delay(250, token); // Simulate work
            // Simulate random failure
            if (new Random().Next(0, 10) < 2)
                throw new Exception("Random failure occurred.");
            return System.IO.File.Exists(filePath) ? "Processed" : "File Not Found";
        }

        private void addToQueueToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //await ProcessSelectedArtifactsAsync(2);
            AddSelectedArtifactsToQueue();
            //MessageBox.Show("Selected files added to the task queue.");
            toolStripStatusLabel1.Text = "Selected files added to the task queue.";
        }
    }

    public class ColumnMetadata
    {
        public string Name { get; set; }
        public Type DataType { get; set; }
        public string DisplayName { get; set; }
        public bool IsEditable { get; set; }
        public bool Visible { get; set; }

        public ColumnMetadata(string name, Type dataType, string displayName, bool isEditable, bool visible = true)
        {
            Name = name;
            DataType = dataType;
            DisplayName = displayName;
            IsEditable = isEditable;
            Visible = visible;
        }
    }

    public static class ArtifactColumns
    {
        public const string SRNO = "SRNo";
        public const string FileName = "FileName";
        public const string FullPath = "FullPath";
        public const string Select = "Select";
        public const string SchemaMatch = "SchemaMatch";
        public const string ColumnNamesMatch = "ColumnNamesMatch";
        public const string ColumnSequenceMatch = "ColumnSequenceMatch";
        public const string DuplicateCheckDone = "DuplicateCheckDone";
        public const string DuplicatesFound = "DuplicatesFound";
        public const string NullsFound = "NullsFound";
        public const string Status = "Status";
    }

    public class FileProcessingTask
    {
        public int Id { get; set; }
        public string FileName { get; set; }
        public string FullPath { get; set; }
        public string Status { get; set; }
        public string Result { get; set; }
    }
}
