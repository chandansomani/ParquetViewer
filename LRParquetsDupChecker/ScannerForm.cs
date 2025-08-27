using System.Data;

namespace LRParquetsDupChecker
{
    public partial class ScannerForm : Form
    {
        Options options;
        DataTable artifactTable;
        List<ColumnMetadata> columns;
        ConfigFile? pkMasterInfo;

        public ScannerForm()
        {
            InitializeComponent();
            artifactTable = new DataTable();

            options = new Options();

            columns = new List<ColumnMetadata>
            {
                new(ArtifactColumns.SRNO, typeof(int), ArtifactColumns.SRNO, true),
                new(ArtifactColumns.Select, typeof(bool), ArtifactColumns.Select, true),
                new(ArtifactColumns.FileName, typeof(string), ArtifactColumns.FileName, false),
                new(ArtifactColumns.FullPath, typeof(string), ArtifactColumns.FullPath, false, false), // hidden
                new(ArtifactColumns.SchemaMatch, typeof(bool), ArtifactColumns.SchemaMatch, false),
                new(ArtifactColumns.ColumnNamesMatch, typeof(bool), ArtifactColumns.ColumnNamesMatch, false),
                new(ArtifactColumns.ColumnSequenceMatch, typeof(bool), ArtifactColumns.ColumnSequenceMatch, false),
                new(ArtifactColumns.DuplicateCheckDone, typeof(bool), ArtifactColumns.DuplicateCheckDone, false),
                new(ArtifactColumns.DuplicatesFound, typeof(int), ArtifactColumns.DuplicatesFound, false),
                new(ArtifactColumns.NullsFound, typeof(int), ArtifactColumns.NullsFound, false),
                new(ArtifactColumns.Status, typeof(string), ArtifactColumns.Status, false)
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
                SuspendLayout();
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
                ResumeLayout();
            }
        }

        private void openFolderToolStripMenuItem_Click(object sender, EventArgs e)
        {
            OpenFolder();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            int selectColIndex = artifactTable.Columns.IndexOf("Select");

            // Loop through each row and set "Select" to true
            foreach (DataRow row in artifactTable.Rows)
            {
                row["Select"] = true;
            }
        }

        private void button2_Click(object sender, EventArgs e)
        {
            int selectColIndex = artifactTable.Columns.IndexOf("Select");

            // Loop through each row and set "Select" to true
            foreach (DataRow row in artifactTable.Rows)
            {
                row["Select"] = false;
            }
        }

        private void button4_Click(object sender, EventArgs e)
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

        private void button3_Click(object sender, EventArgs e)
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
}
