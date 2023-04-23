using InterlinkMapper.Data;

namespace InterlinkMapper.Configs;

public class TransactionTableDefinition : ITableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = "transactions";

	public ColumnDefinition IdColumn { get; set; } = new ColumnDefinition() { ColumnName = "transaction_id", TypeName = "int8", IsPrimaryKey = true };

	public ColumnDefinition DestinationIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "destination_id", TypeName = "int8" };

	public ColumnDefinition DatasourceIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "datasource_id", TypeName = "int8" };

	public ColumnDefinition ArgumentColumn { get; set; } = new ColumnDefinition() { ColumnName = "arguments", TypeName = "int8" };

	public ColumnDefinition TimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public IEnumerable<ColumnDefinition> GetColumns()
	{
		yield return IdColumn;
		yield return DestinationIdColumn;
		yield return DatasourceIdColumn;
		yield return ArgumentColumn;
		yield return TimestampColumn;
	}
}
