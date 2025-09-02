namespace LRPayloadValidatorGUI
{
    public class ColumnAnalysisResult
    {
        public string Expected { get; set; }
        public string ClosestActual { get; set; }
        public string DifferenceType { get; set; }
        public int LevenshteinDistance { get; set; }
    }
}
