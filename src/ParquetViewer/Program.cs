using ParquetViewer.Analytics;
using ParquetViewer.Exceptions;
using ParquetViewer.Helpers;
using System;
using System.IO;
using System.Windows.Forms;

namespace ParquetViewer
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        private static int Main(string[] args)
        {
            string? fileToOpen = null;
            try
            {
                if (args?.Length > 0)
                {
                    if (File.Exists(args[0]))
                    {
                        fileToOpen = args[0];
                    }
                }
            }
            catch (Exception) { /*Swallow Exception*/ }

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);

            //Form must be created after calling SetCompatibleTextRenderingDefault();
            Form mainForm;
            bool isOpeningFile = !string.IsNullOrWhiteSpace(fileToOpen);
            if (isOpeningFile)
                mainForm = new MainForm(fileToOpen!);
            else
                mainForm = new MainForm();

            RouteUnhandledExceptions();

            Application.Run(mainForm);
            return 0;
        }

        private static void RouteUnhandledExceptions()
        {
            //If we're not debugging, route all unhandled exceptions to our top level exception handler
            if (!System.Diagnostics.Debugger.IsAttached)
            {
                // Add the event handler for handling non-UI thread exceptions to the event. 
                AppDomain.CurrentDomain.UnhandledException += new((sender, e) => ExceptionHandler((Exception)e.ExceptionObject));

                // Add the event handler for handling UI thread exceptions to the event.
                Application.ThreadException += new((sender, e) => ExceptionHandler(e.Exception));
            }
        }

        private static void ExceptionHandler(Exception ex)
        {
            ExceptionEvent.FireAndForget(ex);
            MessageBox.Show($"Something went wrong (CTRL+C to copy):{Environment.NewLine}{ex}", ex.Message, MessageBoxButtons.OK, MessageBoxIcon.Error);
        }
        
    }
}
