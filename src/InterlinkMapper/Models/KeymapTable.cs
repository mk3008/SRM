namespace InterlinkMapper.Models;

public class KeymapTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string DestinationSequenceColumn { get; set; } = string.Empty;

	public List<string> DatasourceKeyColumns { get; set; } = new();
}
