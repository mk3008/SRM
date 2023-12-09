namespace InterlinkMapper.Models;

public class KeymapTable : IMaterialInsertable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string DestinationIdColumn { get; set; } = string.Empty;

	public List<string> DatasourceKeyColumns { get; set; } = new();
}
