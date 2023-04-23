namespace InterlinkMapper.Data;

public class DbDatasource : IDatasource
{
	public int DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public IDestination Destination { get; set; } = new DbDestination();

	public string Query { get; set; } = string.Empty;

	public DbTableDefinition KeyMapTable { get; set; } = new();

	public DbTableDefinition RelationMapTable { get; set; } = new();

	public DbTableDefinition HoldTable { get; set; } = new();

	public DbTableDefinition RequestTable { get; set; } = new();

	public bool IsSequence { get; set; } = false;

	public List<string> KeyColumns { get; set; } = new();
}