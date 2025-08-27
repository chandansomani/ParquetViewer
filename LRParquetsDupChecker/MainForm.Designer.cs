namespace LRParquetsDupChecker
{
    partial class MainForm
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            ListViewItem listViewItem1 = new ListViewItem("Chandan");
            ListViewItem listViewItem2 = new ListViewItem("Item 1");
            ListViewItem listViewItem3 = new ListViewItem("Item 2");
            ListViewItem listViewItem4 = new ListViewItem("Item 3");
            ListViewItem listViewItem5 = new ListViewItem("Item 4");
            menuStrip1 = new MenuStrip();
            openFileToolStripMenuItem = new ToolStripMenuItem();
            openFolderToolStripMenuItem = new ToolStripMenuItem();
            checkDuplicatesToolStripMenuItem = new ToolStripMenuItem();
            pKColumnsMetaToolStripMenuItem = new ToolStripMenuItem();
            dataDictionaryToolStripMenuItem = new ToolStripMenuItem();
            refreshToolStripMenuItem = new ToolStripMenuItem();
            addToolStripMenuItem = new ToolStripMenuItem();
            selectPKColumnsToolStripMenuItem = new ToolStripMenuItem();
            checkPKDuplicatesToolStripMenuItem = new ToolStripMenuItem();
            toolsToolStripMenuItem = new ToolStripMenuItem();
            openLogFolderToolStripMenuItem = new ToolStripMenuItem();
            openPKListExcelFileToolStripMenuItem = new ToolStripMenuItem();
            statusStrip1 = new StatusStrip();
            uilblFileName = new Label();
            splitContainer1 = new SplitContainer();
            splitContainer5 = new SplitContainer();
            uiFilesList2 = new ListView();
            FileName = new ColumnHeader();
            PKKnownStatusColumn = new ColumnHeader();
            DuplicatesFound = new ColumnHeader();
            uiFolderPathLabel = new Label();
            button2 = new Button();
            splitContainer3 = new SplitContainer();
            uiColNameList = new CheckedListBox();
            label2 = new Label();
            splitContainer4 = new SplitContainer();
            uiXLSColNameList = new CheckedListBox();
            button1 = new Button();
            btnDiscardChanges = new Button();
            btnSaveToDD = new Button();
            label4 = new Label();
            splitContainer2 = new SplitContainer();
            label1 = new TextBox();
            menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer1).BeginInit();
            splitContainer1.Panel1.SuspendLayout();
            splitContainer1.Panel2.SuspendLayout();
            splitContainer1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer5).BeginInit();
            splitContainer5.Panel1.SuspendLayout();
            splitContainer5.Panel2.SuspendLayout();
            splitContainer5.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer3).BeginInit();
            splitContainer3.Panel1.SuspendLayout();
            splitContainer3.Panel2.SuspendLayout();
            splitContainer3.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer4).BeginInit();
            splitContainer4.Panel1.SuspendLayout();
            splitContainer4.Panel2.SuspendLayout();
            splitContainer4.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer2).BeginInit();
            splitContainer2.Panel1.SuspendLayout();
            splitContainer2.Panel2.SuspendLayout();
            splitContainer2.SuspendLayout();
            SuspendLayout();
            // 
            // menuStrip1
            // 
            menuStrip1.Items.AddRange(new ToolStripItem[] { openFileToolStripMenuItem, openFolderToolStripMenuItem, checkDuplicatesToolStripMenuItem, pKColumnsMetaToolStripMenuItem, selectPKColumnsToolStripMenuItem, checkPKDuplicatesToolStripMenuItem, toolsToolStripMenuItem });
            menuStrip1.Location = new Point(0, 0);
            menuStrip1.Name = "menuStrip1";
            menuStrip1.Size = new Size(1292, 24);
            menuStrip1.TabIndex = 0;
            menuStrip1.Text = "menuStrip1";
            // 
            // openFileToolStripMenuItem
            // 
            openFileToolStripMenuItem.Name = "openFileToolStripMenuItem";
            openFileToolStripMenuItem.ShortcutKeys = Keys.Control | Keys.O;
            openFileToolStripMenuItem.Size = new Size(69, 20);
            openFileToolStripMenuItem.Text = "Open File";
            openFileToolStripMenuItem.Click += openFileToolStripMenuItem_Click;
            // 
            // openFolderToolStripMenuItem
            // 
            openFolderToolStripMenuItem.Name = "openFolderToolStripMenuItem";
            openFolderToolStripMenuItem.Size = new Size(84, 20);
            openFolderToolStripMenuItem.Text = "Open Folder";
            openFolderToolStripMenuItem.Click += openFolderToolStripMenuItem_Click;
            // 
            // checkDuplicatesToolStripMenuItem
            // 
            checkDuplicatesToolStripMenuItem.Name = "checkDuplicatesToolStripMenuItem";
            checkDuplicatesToolStripMenuItem.Size = new Size(207, 20);
            checkDuplicatesToolStripMenuItem.Text = "Check File Duplicates (All Columns)";
            checkDuplicatesToolStripMenuItem.Click += checkDuplicatesToolStripMenuItem_Click;
            // 
            // pKColumnsMetaToolStripMenuItem
            // 
            pKColumnsMetaToolStripMenuItem.DropDownItems.AddRange(new ToolStripItem[] { dataDictionaryToolStripMenuItem, addToolStripMenuItem });
            pKColumnsMetaToolStripMenuItem.Name = "pKColumnsMetaToolStripMenuItem";
            pKColumnsMetaToolStripMenuItem.Size = new Size(97, 20);
            pKColumnsMetaToolStripMenuItem.Text = "DataDictionary";
            // 
            // dataDictionaryToolStripMenuItem
            // 
            dataDictionaryToolStripMenuItem.DropDownItems.AddRange(new ToolStripItem[] { refreshToolStripMenuItem });
            dataDictionaryToolStripMenuItem.Name = "dataDictionaryToolStripMenuItem";
            dataDictionaryToolStripMenuItem.Size = new Size(238, 22);
            dataDictionaryToolStripMenuItem.Text = "Data Dictionary";
            dataDictionaryToolStripMenuItem.Click += dataDictionaryToolStripMenuItem_Click;
            // 
            // refreshToolStripMenuItem
            // 
            refreshToolStripMenuItem.Name = "refreshToolStripMenuItem";
            refreshToolStripMenuItem.Size = new Size(113, 22);
            refreshToolStripMenuItem.Text = "Refresh";
            refreshToolStripMenuItem.Click += refreshToolStripMenuItem_Click;
            // 
            // addToolStripMenuItem
            // 
            addToolStripMenuItem.Name = "addToolStripMenuItem";
            addToolStripMenuItem.Size = new Size(238, 22);
            addToolStripMenuItem.Text = "Add Parquet to Data Dictionary";
            addToolStripMenuItem.Click += addToolStripMenuItem_Click;
            // 
            // selectPKColumnsToolStripMenuItem
            // 
            selectPKColumnsToolStripMenuItem.Name = "selectPKColumnsToolStripMenuItem";
            selectPKColumnsToolStripMenuItem.Size = new Size(115, 20);
            selectPKColumnsToolStripMenuItem.Text = "Select PK from DD";
            selectPKColumnsToolStripMenuItem.Click += selectPKColumnsToolStripMenuItem_Click;
            // 
            // checkPKDuplicatesToolStripMenuItem
            // 
            checkPKDuplicatesToolStripMenuItem.Name = "checkPKDuplicatesToolStripMenuItem";
            checkPKDuplicatesToolStripMenuItem.Size = new Size(127, 20);
            checkPKDuplicatesToolStripMenuItem.Text = "Check PK Duplicates";
            checkPKDuplicatesToolStripMenuItem.Click += checkPKDuplicatesToolStripMenuItem_Click;
            // 
            // toolsToolStripMenuItem
            // 
            toolsToolStripMenuItem.DropDownItems.AddRange(new ToolStripItem[] { openLogFolderToolStripMenuItem, openPKListExcelFileToolStripMenuItem });
            toolsToolStripMenuItem.Name = "toolsToolStripMenuItem";
            toolsToolStripMenuItem.Size = new Size(46, 20);
            toolsToolStripMenuItem.Text = "Tools";
            // 
            // openLogFolderToolStripMenuItem
            // 
            openLogFolderToolStripMenuItem.Name = "openLogFolderToolStripMenuItem";
            openLogFolderToolStripMenuItem.Size = new Size(189, 22);
            openLogFolderToolStripMenuItem.Text = "Open Log Folder";
            openLogFolderToolStripMenuItem.Click += openLogFolderToolStripMenuItem_Click;
            // 
            // openPKListExcelFileToolStripMenuItem
            // 
            openPKListExcelFileToolStripMenuItem.Name = "openPKListExcelFileToolStripMenuItem";
            openPKListExcelFileToolStripMenuItem.Size = new Size(189, 22);
            openPKListExcelFileToolStripMenuItem.Text = "Open PKList Excel File";
            openPKListExcelFileToolStripMenuItem.Click += openPKListExcelFileToolStripMenuItem_Click;
            // 
            // statusStrip1
            // 
            statusStrip1.Location = new Point(0, 710);
            statusStrip1.Name = "statusStrip1";
            statusStrip1.Size = new Size(1292, 22);
            statusStrip1.TabIndex = 1;
            statusStrip1.Text = "statusStrip1";
            // 
            // uilblFileName
            // 
            uilblFileName.BorderStyle = BorderStyle.Fixed3D;
            uilblFileName.Dock = DockStyle.Top;
            uilblFileName.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            uilblFileName.Location = new Point(0, 24);
            uilblFileName.Name = "uilblFileName";
            uilblFileName.Size = new Size(1292, 34);
            uilblFileName.TabIndex = 3;
            uilblFileName.Text = "Parquet File v2 or v3";
            uilblFileName.TextAlign = ContentAlignment.MiddleCenter;
            uilblFileName.Click += uilblFileName_Click;
            // 
            // splitContainer1
            // 
            splitContainer1.Dock = DockStyle.Fill;
            splitContainer1.Location = new Point(0, 0);
            splitContainer1.Name = "splitContainer1";
            // 
            // splitContainer1.Panel1
            // 
            splitContainer1.Panel1.Controls.Add(splitContainer5);
            // 
            // splitContainer1.Panel2
            // 
            splitContainer1.Panel2.Controls.Add(splitContainer3);
            splitContainer1.Size = new Size(1292, 484);
            splitContainer1.SplitterDistance = 292;
            splitContainer1.TabIndex = 6;
            // 
            // splitContainer5
            // 
            splitContainer5.Dock = DockStyle.Fill;
            splitContainer5.Location = new Point(0, 0);
            splitContainer5.Name = "splitContainer5";
            splitContainer5.Orientation = Orientation.Horizontal;
            // 
            // splitContainer5.Panel1
            // 
            splitContainer5.Panel1.Controls.Add(uiFilesList2);
            splitContainer5.Panel1.Controls.Add(uiFolderPathLabel);
            // 
            // splitContainer5.Panel2
            // 
            splitContainer5.Panel2.Controls.Add(button2);
            splitContainer5.Panel2.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            splitContainer5.Size = new Size(292, 484);
            splitContainer5.SplitterDistance = 434;
            splitContainer5.TabIndex = 0;
            // 
            // uiFilesList2
            // 
            uiFilesList2.Columns.AddRange(new ColumnHeader[] { FileName, PKKnownStatusColumn, DuplicatesFound });
            uiFilesList2.Dock = DockStyle.Fill;
            uiFilesList2.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            uiFilesList2.FullRowSelect = true;
            uiFilesList2.GridLines = true;
            listViewItem1.StateImageIndex = 0;
            listViewItem2.StateImageIndex = 0;
            listViewItem3.StateImageIndex = 0;
            listViewItem4.StateImageIndex = 0;
            listViewItem5.StateImageIndex = 0;
            uiFilesList2.Items.AddRange(new ListViewItem[] { listViewItem1, listViewItem2, listViewItem3, listViewItem4, listViewItem5 });
            uiFilesList2.Location = new Point(0, 34);
            uiFilesList2.MultiSelect = false;
            uiFilesList2.Name = "uiFilesList2";
            uiFilesList2.ShowGroups = false;
            uiFilesList2.Size = new Size(292, 400);
            uiFilesList2.TabIndex = 8;
            uiFilesList2.UseCompatibleStateImageBehavior = false;
            uiFilesList2.View = View.Details;
            uiFilesList2.ItemCheck += uiFilesList2_ItemCheck;
            // 
            // FileName
            // 
            FileName.Text = "Parquet File";
            FileName.Width = 260;
            // 
            // PKKnownStatusColumn
            // 
            PKKnownStatusColumn.Text = "PK";
            PKKnownStatusColumn.TextAlign = HorizontalAlignment.Center;
            // 
            // DuplicatesFound
            // 
            DuplicatesFound.Text = "Duplicates";
            DuplicatesFound.TextAlign = HorizontalAlignment.Center;
            // 
            // uiFolderPathLabel
            // 
            uiFolderPathLabel.BorderStyle = BorderStyle.Fixed3D;
            uiFolderPathLabel.Dock = DockStyle.Top;
            uiFolderPathLabel.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            uiFolderPathLabel.Location = new Point(0, 0);
            uiFolderPathLabel.Name = "uiFolderPathLabel";
            uiFolderPathLabel.Size = new Size(292, 34);
            uiFolderPathLabel.TabIndex = 9;
            uiFolderPathLabel.Text = "Open Folder";
            uiFolderPathLabel.TextAlign = ContentAlignment.MiddleCenter;
            // 
            // button2
            // 
            button2.Dock = DockStyle.Fill;
            button2.Location = new Point(0, 0);
            button2.Name = "button2";
            button2.Size = new Size(292, 46);
            button2.TabIndex = 0;
            button2.Text = "Select All";
            button2.UseVisualStyleBackColor = true;
            button2.Click += button2_Click;
            // 
            // splitContainer3
            // 
            splitContainer3.Dock = DockStyle.Fill;
            splitContainer3.Location = new Point(0, 0);
            splitContainer3.Name = "splitContainer3";
            // 
            // splitContainer3.Panel1
            // 
            splitContainer3.Panel1.Controls.Add(uiColNameList);
            splitContainer3.Panel1.Controls.Add(label2);
            // 
            // splitContainer3.Panel2
            // 
            splitContainer3.Panel2.Controls.Add(splitContainer4);
            splitContainer3.Panel2.Controls.Add(label4);
            splitContainer3.Size = new Size(996, 484);
            splitContainer3.SplitterDistance = 495;
            splitContainer3.TabIndex = 9;
            // 
            // uiColNameList
            // 
            uiColNameList.Dock = DockStyle.Fill;
            uiColNameList.Font = new Font("Segoe UI", 14F);
            uiColNameList.FormattingEnabled = true;
            uiColNameList.Location = new Point(0, 34);
            uiColNameList.Name = "uiColNameList";
            uiColNameList.Size = new Size(495, 450);
            uiColNameList.TabIndex = 9;
            // 
            // label2
            // 
            label2.BorderStyle = BorderStyle.Fixed3D;
            label2.Dock = DockStyle.Top;
            label2.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            label2.Location = new Point(0, 0);
            label2.Name = "label2";
            label2.Size = new Size(495, 34);
            label2.TabIndex = 8;
            label2.Text = "Parquet Columns";
            label2.TextAlign = ContentAlignment.MiddleCenter;
            // 
            // splitContainer4
            // 
            splitContainer4.CausesValidation = false;
            splitContainer4.Dock = DockStyle.Fill;
            splitContainer4.FixedPanel = FixedPanel.Panel2;
            splitContainer4.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            splitContainer4.IsSplitterFixed = true;
            splitContainer4.Location = new Point(0, 34);
            splitContainer4.Margin = new Padding(0);
            splitContainer4.Name = "splitContainer4";
            splitContainer4.Orientation = Orientation.Horizontal;
            // 
            // splitContainer4.Panel1
            // 
            splitContainer4.Panel1.Controls.Add(uiXLSColNameList);
            splitContainer4.Panel1.Padding = new Padding(5);
            // 
            // splitContainer4.Panel2
            // 
            splitContainer4.Panel2.Controls.Add(button1);
            splitContainer4.Panel2.Controls.Add(btnDiscardChanges);
            splitContainer4.Panel2.Controls.Add(btnSaveToDD);
            splitContainer4.Panel2MinSize = 32;
            splitContainer4.Size = new Size(497, 450);
            splitContainer4.SplitterDistance = 416;
            splitContainer4.SplitterWidth = 2;
            splitContainer4.TabIndex = 11;
            // 
            // uiXLSColNameList
            // 
            uiXLSColNameList.Dock = DockStyle.Fill;
            uiXLSColNameList.Font = new Font("Segoe UI", 14F);
            uiXLSColNameList.FormattingEnabled = true;
            uiXLSColNameList.Location = new Point(5, 5);
            uiXLSColNameList.Name = "uiXLSColNameList";
            uiXLSColNameList.Size = new Size(487, 406);
            uiXLSColNameList.TabIndex = 19;
            // 
            // button1
            // 
            button1.Dock = DockStyle.Left;
            button1.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            button1.Location = new Point(240, 0);
            button1.Name = "button1";
            button1.Size = new Size(120, 32);
            button1.TabIndex = 22;
            button1.Text = "Close";
            button1.UseVisualStyleBackColor = true;
            button1.Click += button1_Click;
            // 
            // btnDiscardChanges
            // 
            btnDiscardChanges.Dock = DockStyle.Left;
            btnDiscardChanges.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            btnDiscardChanges.Location = new Point(120, 0);
            btnDiscardChanges.Name = "btnDiscardChanges";
            btnDiscardChanges.Size = new Size(120, 32);
            btnDiscardChanges.TabIndex = 20;
            btnDiscardChanges.Text = "Discard";
            btnDiscardChanges.UseVisualStyleBackColor = true;
            // 
            // btnSaveToDD
            // 
            btnSaveToDD.Dock = DockStyle.Left;
            btnSaveToDD.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            btnSaveToDD.Location = new Point(0, 0);
            btnSaveToDD.Name = "btnSaveToDD";
            btnSaveToDD.Size = new Size(120, 32);
            btnSaveToDD.TabIndex = 19;
            btnSaveToDD.Text = "Save";
            btnSaveToDD.UseVisualStyleBackColor = true;
            // 
            // label4
            // 
            label4.BorderStyle = BorderStyle.Fixed3D;
            label4.Dock = DockStyle.Top;
            label4.Font = new Font("Segoe UI", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            label4.Location = new Point(0, 0);
            label4.Name = "label4";
            label4.Size = new Size(497, 34);
            label4.TabIndex = 10;
            label4.Text = "Data Dictionary Parquet Columns";
            label4.TextAlign = ContentAlignment.MiddleCenter;
            // 
            // splitContainer2
            // 
            splitContainer2.Dock = DockStyle.Fill;
            splitContainer2.Location = new Point(0, 58);
            splitContainer2.Name = "splitContainer2";
            splitContainer2.Orientation = Orientation.Horizontal;
            // 
            // splitContainer2.Panel1
            // 
            splitContainer2.Panel1.Controls.Add(splitContainer1);
            // 
            // splitContainer2.Panel2
            // 
            splitContainer2.Panel2.Controls.Add(label1);
            splitContainer2.Size = new Size(1292, 652);
            splitContainer2.SplitterDistance = 484;
            splitContainer2.TabIndex = 7;
            // 
            // label1
            // 
            label1.BackColor = SystemColors.Desktop;
            label1.CausesValidation = false;
            label1.Dock = DockStyle.Fill;
            label1.Font = new Font("Courier New", 14.25F, FontStyle.Regular, GraphicsUnit.Point, 0);
            label1.ForeColor = SystemColors.HighlightText;
            label1.Location = new Point(0, 0);
            label1.Multiline = true;
            label1.Name = "label1";
            label1.ReadOnly = true;
            label1.ScrollBars = ScrollBars.Vertical;
            label1.Size = new Size(1292, 164);
            label1.TabIndex = 0;
            label1.WordWrap = false;
            // 
            // MainForm
            // 
            AutoScaleDimensions = new SizeF(7F, 15F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1292, 732);
            Controls.Add(splitContainer2);
            Controls.Add(uilblFileName);
            Controls.Add(statusStrip1);
            Controls.Add(menuStrip1);
            MainMenuStrip = menuStrip1;
            Name = "MainForm";
            Text = "Levvia Reporting - Parquets Duplicate Checker";
            menuStrip1.ResumeLayout(false);
            menuStrip1.PerformLayout();
            splitContainer1.Panel1.ResumeLayout(false);
            splitContainer1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer1).EndInit();
            splitContainer1.ResumeLayout(false);
            splitContainer5.Panel1.ResumeLayout(false);
            splitContainer5.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer5).EndInit();
            splitContainer5.ResumeLayout(false);
            splitContainer3.Panel1.ResumeLayout(false);
            splitContainer3.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer3).EndInit();
            splitContainer3.ResumeLayout(false);
            splitContainer4.Panel1.ResumeLayout(false);
            splitContainer4.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer4).EndInit();
            splitContainer4.ResumeLayout(false);
            splitContainer2.Panel1.ResumeLayout(false);
            splitContainer2.Panel2.ResumeLayout(false);
            splitContainer2.Panel2.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer2).EndInit();
            splitContainer2.ResumeLayout(false);
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private MenuStrip menuStrip1;
        private StatusStrip statusStrip1;
        private ToolStripMenuItem openFileToolStripMenuItem;
        private ToolStripMenuItem checkDuplicatesToolStripMenuItem;
        private ToolStripMenuItem checkPKDuplicatesToolStripMenuItem;
        private ToolStripMenuItem selectPKColumnsToolStripMenuItem;
        private Label uilblFileName;
        private SplitContainer splitContainer1;
        private SplitContainer splitContainer2;
        private ToolStripMenuItem pKColumnsMetaToolStripMenuItem;
        private TextBox label1;
        private SplitContainer splitContainer3;
        private Label label2;
        private CheckedListBox uiColNameList;
        private Label label4;
        private ToolStripMenuItem addToolStripMenuItem;
        private ToolStripMenuItem dataDictionaryToolStripMenuItem;
        private SplitContainer splitContainer4;
        private CheckedListBox uiXLSColNameList;
        private Button btnDiscardChanges;
        private Button btnSaveToDD;
        private Button button1;
        private ToolStripMenuItem toolsToolStripMenuItem;
        private ToolStripMenuItem openLogFolderToolStripMenuItem;
        private ToolStripMenuItem openPKListExcelFileToolStripMenuItem;
        private SplitContainer splitContainer5;
        private ToolStripMenuItem openFolderToolStripMenuItem;
        private Button button2;
        private Label uiFolderPathLabel;
        private ListView uiFilesList2;
        private ColumnHeader FileName;
        private ColumnHeader PKKnownStatusColumn;
        private ColumnHeader DuplicatesFound;
        private ToolStripMenuItem refreshToolStripMenuItem;
    }
}
