using InterlinkMapper.Materializer;

namespace InterlinkMapper.Models;

public class KeymapTable : IMaterialInsertable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string DestinationSequenceColumn { get; set; } = string.Empty;

	public List<string> DatasourceKeyColumns { get; set; } = new();
}
