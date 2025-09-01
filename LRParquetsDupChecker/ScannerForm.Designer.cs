namespace LRParquetsDupChecker
{
    partial class ScannerForm
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
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
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            menuStrip1 = new MenuStrip();
            openFolderToolStripMenuItem = new ToolStripMenuItem();
            featuresToolStripMenuItem = new ToolStripMenuItem();
            openParquetToolStripMenuItem = new ToolStripMenuItem();
            selectAllToolStripMenuItem = new ToolStripMenuItem();
            deSelectAllToolStripMenuItem = new ToolStripMenuItem();
            loadDataDictionaryToolStripMenuItem = new ToolStripMenuItem();
            verifyFilesWithDDToolStripMenuItem = new ToolStripMenuItem();
            addToQueueToolStripMenuItem = new ToolStripMenuItem();
            startToolStripMenuItem = new ToolStripMenuItem();
            cancelToolStripMenuItem = new ToolStripMenuItem();
            splitContainer1 = new SplitContainer();
            listViewTasks = new ListView();
            dataGridView1 = new DataGridView();
            statusStrip1 = new StatusStrip();
            toolStripStatusLabel1 = new ToolStripStatusLabel();
            toolStripStatusLabel2 = new ToolStripStatusLabel();
            progressBarOverall = new ToolStripProgressBar();
            menuStrip1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)splitContainer1).BeginInit();
            splitContainer1.Panel1.SuspendLayout();
            splitContainer1.Panel2.SuspendLayout();
            splitContainer1.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)dataGridView1).BeginInit();
            statusStrip1.SuspendLayout();
            SuspendLayout();
            // 
            // menuStrip1
            // 
            menuStrip1.Font = new Font("Segoe UI", 12F);
            menuStrip1.Items.AddRange(new ToolStripItem[] { openFolderToolStripMenuItem, featuresToolStripMenuItem, selectAllToolStripMenuItem, deSelectAllToolStripMenuItem, loadDataDictionaryToolStripMenuItem, verifyFilesWithDDToolStripMenuItem, addToQueueToolStripMenuItem, startToolStripMenuItem, cancelToolStripMenuItem });
            menuStrip1.Location = new Point(0, 0);
            menuStrip1.Name = "menuStrip1";
            menuStrip1.Padding = new Padding(8, 3, 0, 3);
            menuStrip1.Size = new Size(1331, 31);
            menuStrip1.TabIndex = 0;
            menuStrip1.Text = "menuStrip1";
            // 
            // openFolderToolStripMenuItem
            // 
            openFolderToolStripMenuItem.Name = "openFolderToolStripMenuItem";
            openFolderToolStripMenuItem.Size = new Size(108, 25);
            openFolderToolStripMenuItem.Text = "Open Folder";
            openFolderToolStripMenuItem.Click += openFolderToolStripMenuItem_Click;
            // 
            // featuresToolStripMenuItem
            // 
            featuresToolStripMenuItem.DropDownItems.AddRange(new ToolStripItem[] { openParquetToolStripMenuItem });
            featuresToolStripMenuItem.Name = "featuresToolStripMenuItem";
            featuresToolStripMenuItem.Size = new Size(81, 25);
            featuresToolStripMenuItem.Text = "Features";
            // 
            // openParquetToolStripMenuItem
            // 
            openParquetToolStripMenuItem.Name = "openParquetToolStripMenuItem";
            openParquetToolStripMenuItem.Size = new Size(175, 26);
            openParquetToolStripMenuItem.Text = "Open Parquet";
            openParquetToolStripMenuItem.Click += openParquetToolStripMenuItem_Click;
            // 
            // selectAllToolStripMenuItem
            // 
            selectAllToolStripMenuItem.Name = "selectAllToolStripMenuItem";
            selectAllToolStripMenuItem.Size = new Size(85, 25);
            selectAllToolStripMenuItem.Text = "Select All";
            selectAllToolStripMenuItem.Click += selectAllToolStripMenuItem_Click;
            // 
            // deSelectAllToolStripMenuItem
            // 
            deSelectAllToolStripMenuItem.Name = "deSelectAllToolStripMenuItem";
            deSelectAllToolStripMenuItem.Size = new Size(104, 25);
            deSelectAllToolStripMenuItem.Text = "DeSelect All";
            deSelectAllToolStripMenuItem.Click += deSelectAllToolStripMenuItem_Click;
            // 
            // loadDataDictionaryToolStripMenuItem
            // 
            loadDataDictionaryToolStripMenuItem.Name = "loadDataDictionaryToolStripMenuItem";
            loadDataDictionaryToolStripMenuItem.Size = new Size(167, 25);
            loadDataDictionaryToolStripMenuItem.Text = "Load Data Dictionary";
            loadDataDictionaryToolStripMenuItem.Click += loadDataDictionaryToolStripMenuItem_Click;
            // 
            // verifyFilesWithDDToolStripMenuItem
            // 
            verifyFilesWithDDToolStripMenuItem.Name = "verifyFilesWithDDToolStripMenuItem";
            verifyFilesWithDDToolStripMenuItem.Size = new Size(157, 25);
            verifyFilesWithDDToolStripMenuItem.Text = "Verify Files with DD";
            verifyFilesWithDDToolStripMenuItem.Click += verifyFilesWithDDToolStripMenuItem_Click;
            // 
            // addToQueueToolStripMenuItem
            // 
            addToQueueToolStripMenuItem.Name = "addToQueueToolStripMenuItem";
            addToQueueToolStripMenuItem.Size = new Size(119, 25);
            addToQueueToolStripMenuItem.Text = "Add To Queue";
            addToQueueToolStripMenuItem.Click += addToQueueToolStripMenuItem_Click;
            // 
            // startToolStripMenuItem
            // 
            startToolStripMenuItem.Name = "startToolStripMenuItem";
            startToolStripMenuItem.Size = new Size(54, 25);
            startToolStripMenuItem.Text = "Start";
            startToolStripMenuItem.Click += btnStart_Click;
            // 
            // cancelToolStripMenuItem
            // 
            cancelToolStripMenuItem.Name = "cancelToolStripMenuItem";
            cancelToolStripMenuItem.Size = new Size(68, 25);
            cancelToolStripMenuItem.Text = "Cancel";
            cancelToolStripMenuItem.Click += btnCancel_Click;
            // 
            // splitContainer1
            // 
            splitContainer1.Dock = DockStyle.Fill;
            splitContainer1.FixedPanel = FixedPanel.Panel1;
            splitContainer1.Location = new Point(0, 31);
            splitContainer1.Margin = new Padding(4);
            splitContainer1.Name = "splitContainer1";
            splitContainer1.Orientation = Orientation.Horizontal;
            // 
            // splitContainer1.Panel1
            // 
            splitContainer1.Panel1.Controls.Add(listViewTasks);
            splitContainer1.Panel1.Padding = new Padding(6);
            // 
            // splitContainer1.Panel2
            // 
            splitContainer1.Panel2.Controls.Add(dataGridView1);
            splitContainer1.Panel2.Padding = new Padding(6);
            splitContainer1.Size = new Size(1331, 651);
            splitContainer1.SplitterDistance = 300;
            splitContainer1.SplitterWidth = 5;
            splitContainer1.TabIndex = 1;
            // 
            // listViewTasks
            // 
            listViewTasks.Dock = DockStyle.Fill;
            listViewTasks.Location = new Point(6, 6);
            listViewTasks.Name = "listViewTasks";
            listViewTasks.Size = new Size(1319, 288);
            listViewTasks.TabIndex = 1;
            listViewTasks.UseCompatibleStateImageBehavior = false;
            // 
            // dataGridView1
            // 
            dataGridView1.AllowUserToAddRows = false;
            dataGridView1.AllowUserToDeleteRows = false;
            dataGridView1.AllowUserToResizeRows = false;
            dataGridView1.ColumnHeadersHeightSizeMode = DataGridViewColumnHeadersHeightSizeMode.AutoSize;
            dataGridView1.Dock = DockStyle.Fill;
            dataGridView1.Location = new Point(6, 6);
            dataGridView1.Margin = new Padding(4);
            dataGridView1.MultiSelect = false;
            dataGridView1.Name = "dataGridView1";
            dataGridView1.RowHeadersVisible = false;
            dataGridView1.RowTemplate.Height = 30;
            dataGridView1.SelectionMode = DataGridViewSelectionMode.FullRowSelect;
            dataGridView1.ShowEditingIcon = false;
            dataGridView1.Size = new Size(1319, 334);
            dataGridView1.TabIndex = 0;
            // 
            // statusStrip1
            // 
            statusStrip1.Items.AddRange(new ToolStripItem[] { toolStripStatusLabel1, toolStripStatusLabel2, progressBarOverall });
            statusStrip1.Location = new Point(0, 682);
            statusStrip1.Name = "statusStrip1";
            statusStrip1.Padding = new Padding(1, 0, 18, 0);
            statusStrip1.Size = new Size(1331, 22);
            statusStrip1.TabIndex = 2;
            statusStrip1.Text = "statusStrip1";
            // 
            // toolStripStatusLabel1
            // 
            toolStripStatusLabel1.Name = "toolStripStatusLabel1";
            toolStripStatusLabel1.Size = new Size(101, 17);
            toolStripStatusLabel1.Text = "Click Open Folder";
            // 
            // toolStripStatusLabel2
            // 
            toolStripStatusLabel2.Name = "toolStripStatusLabel2";
            toolStripStatusLabel2.Size = new Size(909, 17);
            toolStripStatusLabel2.Spring = true;
            // 
            // progressBarOverall
            // 
            progressBarOverall.Name = "progressBarOverall";
            progressBarOverall.Size = new Size(300, 16);
            // 
            // ScannerForm
            // 
            AutoScaleDimensions = new SizeF(9F, 21F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1331, 704);
            Controls.Add(splitContainer1);
            Controls.Add(statusStrip1);
            Controls.Add(menuStrip1);
            Font = new Font("Segoe UI", 12F, FontStyle.Regular, GraphicsUnit.Point, 0);
            MainMenuStrip = menuStrip1;
            Margin = new Padding(4);
            Name = "ScannerForm";
            Text = "Scanner Form";
            menuStrip1.ResumeLayout(false);
            menuStrip1.PerformLayout();
            splitContainer1.Panel1.ResumeLayout(false);
            splitContainer1.Panel2.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)splitContainer1).EndInit();
            splitContainer1.ResumeLayout(false);
            ((System.ComponentModel.ISupportInitialize)dataGridView1).EndInit();
            statusStrip1.ResumeLayout(false);
            statusStrip1.PerformLayout();
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private MenuStrip menuStrip1;
        private ToolStripMenuItem openFolderToolStripMenuItem;
        private SplitContainer splitContainer1;
        private DataGridView dataGridView1;
        private StatusStrip statusStrip1;
        private ToolStripStatusLabel toolStripStatusLabel1;
        private ToolStripMenuItem featuresToolStripMenuItem;
        private ToolStripMenuItem openParquetToolStripMenuItem;
        private ToolStripMenuItem selectAllToolStripMenuItem;
        private ToolStripMenuItem deSelectAllToolStripMenuItem;
        private ToolStripMenuItem loadDataDictionaryToolStripMenuItem;
        private ToolStripMenuItem verifyFilesWithDDToolStripMenuItem;
        private ListView listViewTasks;
        private ToolStripMenuItem startToolStripMenuItem;
        private ToolStripMenuItem cancelToolStripMenuItem;
        private ToolStripMenuItem addToQueueToolStripMenuItem;
        private ToolStripStatusLabel toolStripStatusLabel2;
        private ToolStripProgressBar progressBarOverall;
    }
}