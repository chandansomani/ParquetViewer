using System.Text.Json.Serialization;

namespace ParquetDuplicateFinder
{
    [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true)]
    [JsonSerializable(typeof(ConfigFile))]
    [JsonSerializable(typeof(ParquetFileConfig))]
    [JsonSerializable(typeof(ColumnConfig))]
    [JsonSerializable(typeof(Dictionary<string, ParquetFileConfig>))]
    [JsonSerializable(typeof(List<ColumnConfig>))]
    public partial class ConfigJsonContext : JsonSerializerContext
    {
    }
}