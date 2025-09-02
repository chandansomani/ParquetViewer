namespace LRPayloadValidatorGUI
{
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
}