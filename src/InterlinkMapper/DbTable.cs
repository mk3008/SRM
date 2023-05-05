namespace InterlinkMapper;

public class DbTable : IDbTable
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = string.Empty;

	public List<string> Columns { get; set; } = new();

	IEnumerable<string> IDbTable.Columns => Columns;

	//List<ColumnDefinition> ITableDefinition.Columns => this.Columns;

	//public string TableFullName => string.IsNullOrEmpty(SchemaName) ? TableName : SchemaName + "." + TableName;
}