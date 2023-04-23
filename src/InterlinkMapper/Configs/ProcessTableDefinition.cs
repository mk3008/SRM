using InterlinkMapper.Data;

namespace InterlinkMapper.Configs;

public class ProcessTableDefinition : ITableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = "processes";

	public ColumnDefinition IdColumn { get; set; } = new ColumnDefinition() { ColumnName = "processe_id", TypeName = "int8", IsPrimaryKey = true };

	public ColumnDefinition TransactionIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "transaction_id", TypeName = "int8" };

	public ColumnDefinition DestinationIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "destination_id", TypeName = "int8" };

	public ColumnDefinition DatasourceIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "datasource_id", TypeName = "int8" };

	public ColumnDefinition TimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public IEnumerable<ColumnDefinition> GetColumns()
	{
		yield return IdColumn;
		yield return TransactionIdColumn;
		yield return DestinationIdColumn;
		yield return DatasourceIdColumn;
		yield return TimestampColumn;
	}
}
