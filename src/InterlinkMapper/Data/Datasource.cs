namespace InterlinkMapper.Data;

public class Datasource
{
	public int DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public Destination Destination { get; set; } = new();

	public string Query { get; set; } = string.Empty;

	public DbTable ProcessMapTable => Destination.ProcessMap;

	public DbTableDefinition KeyMapTable { get; set; } = new();

	public DbTableDefinition RelationMapTable { get; set; } = new();

	public DbTableDefinition HoldTable { get; set; } = new();

	public DbTableDefinition RequestTable { get; set; } = new();

	public bool IsSequence { get; set; } = false;

	public List<string> KeyColumns { get; set; } = new();

	public bool HasKeyMapTable => (string.IsNullOrEmpty(KeyMapTable.GetTableFullName()) ? false : true);
	public bool HasRelationMapTable => (string.IsNullOrEmpty(RelationMapTable.GetTableFullName()) ? false : true);
	public bool HasHoldTable => (string.IsNullOrEmpty(HoldTable.GetTableFullName()) ? false : true);
	public bool HasRequestTable => (string.IsNullOrEmpty(RequestTable.GetTableFullName()) ? false : true);
}