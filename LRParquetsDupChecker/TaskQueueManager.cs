namespace LRParquetsDupChecker
{
    public class TaskQueueManager
    {
        private ListView _listView;
        private ToolStripProgressBar _progressBar;
        private List<FileProcessingTask> _taskQueue = new List<FileProcessingTask>();
        private int _tasksCompleted = 0;
        private Control _invokeControl; // Reference to a UI control for Invoke

        public TaskQueueManager(ListView listView, ToolStripProgressBar progressBar, Control invokeControl)
        {
            _listView = listView;
            _progressBar = progressBar;
            _invokeControl = invokeControl;
            InitializeListView();
        }

        private void InitializeListView()
        {
            // Use Invoke to ensure UI operations happen on the main thread
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action(() => InitializeListView()));
                return;
            }

            _listView.View = View.Details;
            _listView.FullRowSelect = true;
            _listView.Columns.Add("ID", 50);
            _listView.Columns.Add("File Name", 200);
            _listView.Columns.Add("Status", 100);
            _listView.Columns.Add("Validation Result", 600);
            _listView.Columns.Add("Result", 600);

            _progressBar.Minimum = 0;
            _progressBar.Value = 0;
            _progressBar.Step = 1;
        }

        public void AddTasks(List<string> files)
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action<List<string>>(AddTasks), files);
                return;
            }

            int nextId = _taskQueue.Count + 1;

            foreach (var file in files)
            {
                if (File.Exists(file))
                {
                    var task = CreateTask(nextId++, file);
                    _taskQueue.Add(task);
                    AddTaskToListView(task);
                }
            }

            _progressBar.Maximum = _taskQueue.Count(t => t.Status == "Pending");
            _progressBar.Value = 0;
        }

        private FileProcessingTask CreateTask(int id, string filePath)
        {
            return new FileProcessingTask
            {
                Id = id,
                FileName = Path.GetFileName(filePath),
                FullPath = filePath,
                Status = "Pending",
                Result = ""
            };
        }

        private void AddTaskToListView(FileProcessingTask task)
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action<FileProcessingTask>(AddTaskToListView), task);
                return;
            }

            var item = new ListViewItem(new[]
            {
                task.Id.ToString(),
                task.FileName,
                task.Status,
                task.ValidationStatus,
                task.Result
            })
            {
                Tag = task
            };
            _listView.Items.Add(item);
        }

        public void UpdateTaskStatus(FileProcessingTask task, string status, string result)
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action<FileProcessingTask, string, string>(UpdateTaskStatus), task, status, result);
                return;
            }

            task.Status = status;
            task.Result = result;

            foreach (ListViewItem item in _listView.Items)
            {
                if (item.Tag == task)
                {
                    item.SubItems[2].Text = status;
                    item.SubItems[4].Text = result;
                    break;
                }
            }
        }

        public void UpdateValidationStatus(FileProcessingTask task, bool status ,string result)
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action<FileProcessingTask, bool, string>(UpdateValidationStatus), task, status, result);
                return;
            }
            
            task.ValidationCheck = status;
            task.Result = result;

            foreach (ListViewItem item in _listView.Items)
            {
                if (item.Tag == task)
                {
                    item.SubItems[3].Text = result;
                    break;
                }
            }
        }

        public void UpdateProgress()
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action(UpdateProgress));
                return;
            }

            _tasksCompleted++;
            if (_tasksCompleted <= _progressBar.Maximum)
                _progressBar.Value = _tasksCompleted;
        }

        public void ResetProgress()
        {
            if (_invokeControl.InvokeRequired)
            {
                _invokeControl.Invoke(new Action(ResetProgress));
                return;
            }

            _tasksCompleted = 0;
            _progressBar.Value = 0;
        }

        public IEnumerable<FileProcessingTask> GetPendingTasks()
        {
            return _taskQueue.Where(t => t.Status == "Pending");
        }
        public IEnumerable<FileProcessingTask> GetValidationPendingTasks()
        {
            return _taskQueue.Where(t => t.ValidationCheck == false);
        }
    }
}

/* Working of Invoke in this context:
 Key Points:
    InvokeRequired: Always check if you're on the UI thread before accessing UI controls

    Invoke: Use Control.Invoke() or Control.BeginInvoke() to marshal calls to the UI thread

    Pass Control Reference: The TaskQueueManager needs a reference to a UI control to use for invoking

    Thread Safety: All UI updates should be wrapped in Invoke checks

    The error occurs because background threads (from Task.Run) cannot directly access UI controls that were created on the main thread. The Invoke method ensures the code runs on the proper thread.
 
 */