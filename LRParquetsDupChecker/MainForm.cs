using System.Data;
using System.Text;
using System.Security.Cryptography;
using System.Diagnostics;
using Color = System.Drawing.Color;
using LRPayloadValidatorGUI;

namespace LRParquetsDupChecker
{
    public partial class MainForm : Form
    {
        Options options;
        ConfigFile? pkMasterInfo;

        public MainForm()
        {
            InitializeComponent();
            options = new Options();

            splitContainer1.Panel1Collapsed = true;       //FolderFiles And Body
            splitContainer3.Panel2Collapsed = true;
            //splitContainer5.Panel2Collapsed = true;
            addToolStripMenuItem.Enabled = false;
            btnSaveToDD.Visible = false;
            btnSaveToDD.Enabled = false;
            btnDiscardChanges.Visible = false;
            btnDiscardChanges.Enabled = false;

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

        private void openFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            OpenFileDialog openFileDialog = new OpenFileDialog();
            openFileDialog.Filter = "v3 Parquets|*.parquet;*.parquet.gz;|v2 Parquets|*.csv;*.csv.gz;|All files (*.*)|*.*";
            openFileDialog.FilterIndex = 1;
            openFileDialog.RestoreDirectory = true;

            if (openFileDialog.ShowDialog() == DialogResult.OK)
            {
                //Options options = new Options();
                options.FilePath = openFileDialog.FileName;

                //TODO - Check File  is available to read 

                string fileName = Path.GetFileName(options.FilePath);

                UpdateOptionsForFile(options, fileName);

                List<string> parquetColumns = new List<string>();
                if (options.IsCsv)
                {
                    parquetColumns = Task.Run(() => CsvOperations.GetCSVColumns(options)).GetAwaiter().GetResult();
                }
                else
                {
                    parquetColumns = Task.Run(() => ParquetOperations.GetParquetColumns(options)).GetAwaiter().GetResult();
                }


                uiColNameList.Items.Clear();
                uiColNameList.Items.AddRange(parquetColumns.ToArray());

                uilblFileName.Text = fileName;
                if (pkMasterInfo.ParquetFiles.ContainsKey(fileName))
                {
                    Log($"Info found in pklist.xlxs\n");
                    List<string> parquetColumnsInXls = new List<string>();
                    parquetColumnsInXls = pkMasterInfo.ParquetFiles[fileName].Columns.Select(x => x.Name).ToList();

                    List<string> parquetPKColumnsFromXls = new List<string>();
                    var columns = pkMasterInfo.ParquetFiles[fileName].Columns;
                    if (columns != null)
                    {
                        parquetPKColumnsFromXls = columns
                            .Where(c => c.IsPrimaryKey)
                            .Select(c => c.Name)
                            .ToList();
                    }

                    uiXLSColNameList.Items.Clear();
                    uiXLSColNameList.Items.AddRange(parquetColumnsInXls.ToArray());

                    foreach (string primaryKeyColumn in parquetPKColumnsFromXls)
                    {
                        int index = uiXLSColNameList.Items.IndexOf(primaryKeyColumn);
                        if (index != -1)
                        {
                            uiXLSColNameList.SetItemChecked(index, true);
                        }
                    }
                }
                else
                {
                    Log($"Info not found in pklist.xlxs\n");
                    uiXLSColNameList.Items.Clear();
                    addToolStripMenuItem.Enabled = true;
                }
            }
        }

        private void selectPKColumnsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (options.FilePath == null)
            {
                Log("Select Some File\n");
                return;
            }
            foreach (int index in uiColNameList.CheckedIndices)
            {
                uiColNameList.SetItemChecked(index, false);
            }

            options.PrimaryKeyColumns = PKConfigManager.LoadPrimaryKeyColumns(options.ConfigFilePath, options.FilePath);

            string fileName = Path.GetFileName(options.FilePath);

            if (options.PrimaryKeyColumns == null)
            {
                Log("Parquet Info not avialable in pklist.xslx\n");
                return;
            }
            List<string> primaryKeyColumns = options.PrimaryKeyColumns[fileName];

            //label1.Text = string.Join(", ", primaryKeyColumns);

            foreach (string primaryKeyColumn in primaryKeyColumns)
            {
                int index = uiColNameList.Items.IndexOf(primaryKeyColumn);
                if (index != -1)
                {
                    uiColNameList.SetItemChecked(index, true);
                }
            }
        }

        private void checkDuplicatesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (options.FilePath == null)
            {
                Log("Select Some File\n");
                return;
            }
            List<string> primaryKeyColumns = new List<string>();
            foreach (var item in uiColNameList.Items)
            {
                primaryKeyColumns.Add(item.ToString());
            }

            DataTable dataTable;
            if (options.IsCsv)
            {
                dataTable = Task.Run(() => CsvOperations.LoadCsv(options, primaryKeyColumns)).GetAwaiter().GetResult();
            }
            else
            {
                dataTable = Task.Run(() => ParquetOperations.LoadParquet(options, primaryKeyColumns)).GetAwaiter().GetResult();
            }


            options.Verbose = true;
            options.Limit = 10;
            FindDuplicates(dataTable, primaryKeyColumns, options);
        }

        private void checkPKDuplicatesToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (options.FilePath == null)
            {
                Log("Select Some File\n");
                return;
            }
            List<string> primaryKeyColumns = new List<string>();
            foreach (var item in uiColNameList.CheckedItems)
            {
                primaryKeyColumns.Add(item.ToString());
            }

            DataTable dataTable;
            if (options.IsCsv)
            {
                dataTable = Task.Run(() => CsvOperations.LoadCsv(options, primaryKeyColumns)).GetAwaiter().GetResult();
            }
            else
            {
                dataTable = Task.Run(() => ParquetOperations.LoadParquet(options, primaryKeyColumns)).GetAwaiter().GetResult();
            }

            options.Verbose = true;
            options.Limit = 10;
            FindDuplicates(dataTable, primaryKeyColumns, options);
        }

        private void button1_Click(object sender, EventArgs e)
        {
            splitContainer3.Panel2Collapsed = true;
        }
        private void dataDictionaryToolStripMenuItem_Click(object sender, EventArgs e)
        {
            //label1.Text = $"{pkMasterInfo.ParquetFiles.Count()} Files PK Information is loaded from pklist.xlsx";
            Log($"{pkMasterInfo.ParquetFiles.Count()} Files PK Information is loaded from pklist.xlsx\n");
            splitContainer3.Panel2Collapsed = !splitContainer3.Panel2Collapsed;
        }



        private static void UpdateOptionsForFile(Options options, string filePath)
        {
            //options.FilePath = filePath;

            if (filePath.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                filePath.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase))
            {
                options.IsCsv = true;
                options.Delimiter = options.Delimiter != '\0' ? options.Delimiter : ',';
                options.HasHeader = true;
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
        }

        private bool FindDuplicates(DataTable dataTable, List<string> columnsToUse, Options options)
        {
            if (options.Verbose) Program.Log($"Finding duplicates using columns: {string.Join(", ", columnsToUse)}");
            Log($"Finding duplicates using columns: {string.Join(", ", columnsToUse)}");

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
                Program.Log($"[{Path.GetFileName(options.FilePath)}] : No duplicates found.\n");
                Log($"[{Path.GetFileName(options.FilePath)}] : No duplicates found.\n");
                return false;
            }

            Program.Log($"Found {duplicates.Count} duplicate groups.");
            Program.Log("═══════ Duplicate Summary ══════");
            Program.Log($"{"Group #",-8} | {"Count",-6} | {"Row #",-12} | Record");
            Program.Log(new string('─', 8 + 3 + 6 + 3 + 12 + 3 + columnsToUse.Count * 20));

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
                Program.Log($"{i + 1,-8} | {group.Value.Count,-6} | {sample.Position,-12} | {sampleValues}");
                Log($"{i + 1,-8} | {group.Value.Count,-6} | {sample.Position,-12} | {sampleValues}");
            }

            int totalDuplicates = duplicates.Sum(g => g.Value.Count - 1);
            Program.Log($"\nSummary: Found {totalDuplicates} duplicate records in {duplicates.Count} groups.");
            Program.LogLineBreak();

            Log($"\nSummary: Found {totalDuplicates} duplicate records in {duplicates.Count} groups.");
            Log("\n");
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

        private void Log(string message, bool newLine = true)
        {
            message = message.Replace("\n", Environment.NewLine);
            label1.Text += ($"{DateTime.Now}: {message}{(newLine ? Environment.NewLine : string.Empty)}");
            label1.SelectionStart = label1.Text.Length;
            label1.ScrollToCaret();
        }

        private void addToolStripMenuItem_Click(object sender, EventArgs e)
        {
            List<string> primaryKeyColumns = new List<string>();
            foreach (var item in uiColNameList.Items)
            {
                primaryKeyColumns.Add(item.ToString());
            }

            uiXLSColNameList.Items.Clear();
            uiXLSColNameList.Items.AddRange(primaryKeyColumns.ToArray());
            btnSaveToDD.Visible = true;
            btnSaveToDD.Enabled = true;

        }

        private void openLogFolderToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (Directory.Exists(options.LogFolder))
            {
                Process.Start("explorer.exe", options.LogFolder);
            }
        }

        private void openPKListExcelFileToolStripMenuItem_Click(object sender, EventArgs e)
        {
            var FullConfigFilePath = (Environment.CurrentDirectory + '\\' + options.ConfigFilePath);
            if (File.Exists(FullConfigFilePath))
            {
                Process.Start("explorer.exe", FullConfigFilePath);
            }
        }

        private void openFolderToolStripMenuItem_Click(object sender, EventArgs e)
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
                splitContainer1.Panel1Collapsed = true;
                this.Text = "Levvia Reporting - Parquets Duplicate Checker";
                uiFilesList2.Items.Clear();

            }
            else
            {
                this.Text = this.Text + " - " + folderPath;
                splitContainer1.Panel1Collapsed = false;
                splitContainer1.SplitterDistance = 500;
                /** Load all valid parquet Filenames of current dir to fileList UIComponent **/
                filesToProcess = Directory.EnumerateFiles(folderPath, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                               f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase));

                uiFilesList2.Items.Clear();

                foreach (var file in filesToProcess)
                {
                    uiFilesList2.Items.Add(Path.GetFileName(file));
                }

            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            bool allChecked = true;
            foreach (ListViewItem item in uiFilesList2.Items)
            {
                if (!item.Checked)
                {
                    allChecked = false;
                    break;
                }
            }

            if (allChecked)
            {
                // Deselect all
                foreach (ListViewItem item in uiFilesList2.Items)
                {
                    item.Checked = false;
                }
                button2.Text = "Select All";
            }
            else
            {
                // Select all
                foreach (ListViewItem item in uiFilesList2.Items)
                {
                    item.Checked = true;
                }
                button2.Text = "Deselect All";
            }
        }

        private void refreshToolStripMenuItem_Click(object sender, EventArgs e)
        {
            if (File.Exists(PKConfigManager.DefaultConfigFile))
            {
                options.ConfigFilePath = PKConfigManager.DefaultConfigFile;
                pkMasterInfo = PKConfigManager.LoadFromExcel(options.ConfigFilePath);
                Log($"{pkMasterInfo.ParquetFiles.Count()} Files PK Information is loaded from pklist.xlsx\n");
            }
        }

        private void uilblFileName_Click(object sender, EventArgs e)
        {
            Clipboard.SetText(uilblFileName.Text);
        }

        private void uiFilesList2_ItemCheck(object sender, ItemCheckEventArgs e)
        {
            ListView listView = sender as ListView;
            if (listView == null) return;

            // Get the item being checked/unchecked
            ListViewItem item = listView.Items[e.Index];

            // Use e.NewValue to determine the new state
            if (e.NewValue == CheckState.Checked)
            {
                // Set background to green and foreground to white
                item.BackColor = Color.Green;
                item.ForeColor = Color.White;
            }
            else
            {
                // Set background to gray and foreground to black
                item.BackColor = Color.LightGray;
                item.ForeColor = Color.Black;
            }
        }
    }
}
