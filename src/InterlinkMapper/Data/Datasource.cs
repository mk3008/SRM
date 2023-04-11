namespace InterlinkMapper.Data;

public class Datasource
{
	public int DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public Destination Destination { get; set; } = new();

	public string Query { get; set; } = string.Empty;

	public DbTable KeyMapTable { get; set; } = new();

	public DbTable RelationTable { get; set; } = new();

	public bool IsSequence { get; set; } = false;

	public List<string> KeyColumns { get; set; } = new();
}