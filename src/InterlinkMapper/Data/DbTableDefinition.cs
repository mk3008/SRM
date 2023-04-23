namespace InterlinkMapper.Data;

public class DbTableDefinition : IDbTableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = string.Empty;

	public string TableFullName => string.IsNullOrEmpty(SchemaName) ? TableName : SchemaName + "." + TableName;

	public List<ColumnDefinition> ColumnDefinitions { get; set; } = new();

	public IEnumerable<string> Columns => ColumnDefinitions.Select(x => x.ColumnName);

	IEnumerable<ColumnDefinition> IDbTableDefinition.ColumnDefinitions => ColumnDefinitions;

	//public IEnumerable<ColumnDefinition> ColumnDefinitions => throw new NotImplementedException();

	//IEnumerable<string> ITable.Columns => throw new NotImplementedException();

	//public IEnumerable<ColumnDefinition> GetColumns() => Columns;
}
