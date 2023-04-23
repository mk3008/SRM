using InterlinkMapper.Data;

namespace InterlinkMapper.Configs;

public class DatasourceTableDefinition : ITableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = "datasources";

	public ColumnDefinition IdColumn { get; set; } = new ColumnDefinition() { ColumnName = "dataource_id", TypeName = "serial8", IsPrimaryKey = true, IsAutoNumber = true };

	public ColumnDefinition DestinationIdColumn { get; set; } = new ColumnDefinition() { ColumnName = "destination_id", TypeName = "int8" };

	public ColumnDefinition NameColumn { get; set; } = new ColumnDefinition() { ColumnName = "dataource_name", TypeName = "text" };

	public ColumnDefinition DescriptionColumn { get; set; } = new ColumnDefinition() { ColumnName = "description", TypeName = "text" };

	public ColumnDefinition QueryColumn { get; set; } = new ColumnDefinition() { ColumnName = "query", TypeName = "text" };

	public ColumnDefinition KeyColumnsColumn { get; set; } = new ColumnDefinition() { ColumnName = "key_columns", TypeName = "text" };

	public ColumnDefinition KeyMapTableNameColumn { get; set; } = new ColumnDefinition() { ColumnName = "key_map_table_name", TypeName = "text" };

	public ColumnDefinition RelationMapTableNameColumn { get; set; } = new ColumnDefinition() { ColumnName = "relation_map_table_name", TypeName = "text" };

	public ColumnDefinition RequestTableNameColumn { get; set; } = new ColumnDefinition() { ColumnName = "request_table_name", TypeName = "text" };

	public ColumnDefinition HoldTableNameColumn { get; set; } = new ColumnDefinition() { ColumnName = "hold_table_name", TypeName = "text" };

	public ColumnDefinition CreateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public ColumnDefinition UpdateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "updated_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public IEnumerable<ColumnDefinition> GetColumns()
	{
		yield return IdColumn;
		yield return DestinationIdColumn;

		yield return NameColumn;
		yield return DescriptionColumn;
		yield return QueryColumn;
		yield return KeyColumnsColumn;

		yield return CreateTimestampColumn;
		yield return UpdateTimestampColumn;
	}
}
