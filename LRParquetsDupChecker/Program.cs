namespace LRParquetsDupChecker
{
    internal static class Program
    {
        private static string? logFilePath;
        /// <summary>
        ///  The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            InitializeLogging(new Options());
            // To customize application configuration such as set high DPI settings or default font,
            // see https://aka.ms/applicationconfiguration.
            ApplicationConfiguration.Initialize();
            //Application.Run(new MainForm());
            Application.Run(new ScannerForm());
            //Application.Run(new ScannerFormNew());
            //Application.Run(new TaskQueueForm());
        }
        public static void Log(string message, bool newLine = true)
        {
            File.AppendAllText(logFilePath, $"{DateTime.Now}: {message}{(newLine ? "\n" : string.Empty)}");
        }
        public static void LogLineBreak() => File.AppendAllText(logFilePath, Environment.NewLine);
        private static void InitializeLogging(Options options)
        {
            Directory.CreateDirectory(options.LogFolder);
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            logFilePath = Path.Combine(options.LogFolder, $"Run_{timestamp}.log");                        
            LogLineBreak();
        }
    }    
}