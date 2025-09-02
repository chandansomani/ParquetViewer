namespace LRParquetsDupChecker
{
    public class FileProcessingTask
    {
        public int Id { get; set; }
        public string FileName { get; set; }
        public string FullPath { get; set; }
        public bool ValidationCheck { get; set; }
        public string ValidationStatus { get; set; }
        public string Status { get; set; }
        public string Result { get; set; }
    }
}