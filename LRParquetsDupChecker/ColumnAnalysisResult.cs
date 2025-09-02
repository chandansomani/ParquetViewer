using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
