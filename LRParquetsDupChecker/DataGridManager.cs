using System.Data;

namespace LRParquetsDupChecker
{
    public class DataGridManager
    {
        private DataTable _artifactTable;
        private List<ColumnMetadata> _columns;
        private DataGridView _dataGridView;

        public DataGridManager(DataGridView dataGridView)
        {
            _dataGridView = dataGridView;
            InitializeDataTable();
            ConfigureDataGridView();
        }

        private void InitializeDataTable()
        {
            _artifactTable = new DataTable();
            _columns = new List<ColumnMetadata>
            {
                new(ArtifactColumns.SRNO, typeof(int), ArtifactColumns.SRNO, true),
                new(ArtifactColumns.Select, typeof(bool), ArtifactColumns.Select, true),
                new(ArtifactColumns.FileName, typeof(string), ArtifactColumns.FileName, false),
                new(ArtifactColumns.Status, typeof(string), ArtifactColumns.Status, false),
                new(ArtifactColumns.FullPath, typeof(string), ArtifactColumns.FullPath, false, false),
                new(ArtifactColumns.SchemaMatch, typeof(bool), ArtifactColumns.SchemaMatch, false),
                new(ArtifactColumns.ColumnNamesMatch, typeof(bool), ArtifactColumns.ColumnNamesMatch, false),
                new(ArtifactColumns.ColumnSequenceMatch, typeof(bool), ArtifactColumns.ColumnSequenceMatch, false),
                new(ArtifactColumns.DuplicateCheckDone, typeof(bool), ArtifactColumns.DuplicateCheckDone, false),
                new(ArtifactColumns.DuplicatesFound, typeof(int), ArtifactColumns.DuplicatesFound, false),
                new(ArtifactColumns.NullsFound, typeof(int), ArtifactColumns.NullsFound, false),
            };

            foreach (var col in _columns)
            {
                _artifactTable.Columns.Add(col.Name, col.DataType);
            }
        }

        private void ConfigureDataGridView()
        {
            _dataGridView.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.AllCells;
            _dataGridView.DataSource = _artifactTable;

            _dataGridView.Columns[ArtifactColumns.SRNO].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            _dataGridView.Columns[ArtifactColumns.DuplicatesFound].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            _dataGridView.Columns[ArtifactColumns.NullsFound].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleRight;
            _dataGridView.Columns[ArtifactColumns.Status].DefaultCellStyle.Alignment = DataGridViewContentAlignment.MiddleCenter;

            foreach (var col in _columns)
            {
                var gridCol = _dataGridView.Columns[col.Name];
                if (gridCol != null)
                {
                    gridCol.HeaderText = col.DisplayName;
                    gridCol.ReadOnly = !col.IsEditable;
                    gridCol.Visible = col.Visible;
                }
            }
        }

        public void LoadFilesFromFolder(string folderPath)
        {
            _artifactTable.Rows.Clear();
            
            var filesToProcess = Directory.EnumerateFiles(folderPath, "*.*", SearchOption.TopDirectoryOnly)
                .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                           f.EndsWith(".csv.gz", StringComparison.OrdinalIgnoreCase) ||
                           f.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) ||
                           f.EndsWith(".parquet.gz", StringComparison.OrdinalIgnoreCase));

            int i = 1;
            foreach (var file in filesToProcess)
            {
                AddFileToTable(file, i++);
            }
        }

        private void AddFileToTable(string filePath, int srNo)
        {
            var rowValues = new object[_columns.Count];

            for (int colIdx = 0; colIdx < _columns.Count; colIdx++)
            {
                var col = _columns[colIdx];
                rowValues[colIdx] = GetColumnValue(col.Name, filePath, srNo);
            }

            _artifactTable.Rows.Add(rowValues);
        }

        private object GetColumnValue(string columnName, string filePath, int srNo)
        {
            return columnName switch
            {
                ArtifactColumns.SRNO => srNo,
                ArtifactColumns.Select => false,
                ArtifactColumns.FileName => Path.GetFileName(filePath),
                ArtifactColumns.FullPath => filePath,
                ArtifactColumns.SchemaMatch or 
                ArtifactColumns.ColumnNamesMatch or 
                ArtifactColumns.ColumnSequenceMatch or 
                ArtifactColumns.DuplicateCheckDone => false,
                ArtifactColumns.DuplicatesFound or 
                ArtifactColumns.NullsFound => 0,
                ArtifactColumns.Status => "Pending",
                _ => DBNull.Value
            };
        }

        public List<string> GetSelectedFiles()
        {
            var selectedFiles = new List<string>();
            
            foreach (DataRow row in _artifactTable.Rows)
            {
                if (row[ArtifactColumns.Select] is bool isSelected && isSelected)
                {
                    string filePath = row[ArtifactColumns.FullPath]?.ToString();
                    if (!string.IsNullOrEmpty(filePath))
                        selectedFiles.Add(filePath);
                }
            }
            
            return selectedFiles;
        }

        public void SelectAll(bool select)
        {
            foreach (DataRow row in _artifactTable.Rows)
            {
                row[ArtifactColumns.Select] = select;
            }
        }
    }
}