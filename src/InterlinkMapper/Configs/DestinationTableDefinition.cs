using InterlinkMapper.Data;

namespace InterlinkMapper.Configs;

public class DestinationTableDefinition : ITableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = "destinations";

	public ColumnDefinition IdColumn { get; set; } = new ColumnDefinition() { ColumnName = "destination_is", TypeName = "serial8", IsPrimaryKey = true, IsAutoNumber = true };

	public ColumnDefinition SchemaColumn { get; set; } = new ColumnDefinition() { ColumnName = "schema_name", TypeName = "text" };

	public ColumnDefinition TableColumn { get; set; } = new ColumnDefinition() { ColumnName = "table_name", TypeName = "text" };

	public ColumnDefinition ColumnsDefinitionColumn { get; set; } = new ColumnDefinition() { ColumnName = "columns_definition", TypeName = "text" };

	public ColumnDefinition SequenceDefinitionColumn { get; set; } = new ColumnDefinition() { ColumnName = "sequence_definition", TypeName = "text" };

	public ColumnDefinition ReverseDefinitionColumn { get; set; } = new ColumnDefinition() { ColumnName = "reverse_definition", TypeName = "text" };

	public ColumnDefinition ProcessMapTableNameColumn { get; set; } = new ColumnDefinition() { ColumnName = "process_map_table_name", TypeName = "text" };

	public ColumnDefinition DescriptionColumn { get; set; } = new ColumnDefinition() { ColumnName = "description", TypeName = "text" };

	public ColumnDefinition CreateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public ColumnDefinition UpdateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "updated_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public IEnumerable<ColumnDefinition> GetColumns()
	{
		yield return IdColumn;

		yield return SchemaColumn;
		yield return TableColumn;
		yield return ColumnsDefinitionColumn;

		yield return SequenceDefinitionColumn;
		yield return ReverseDefinitionColumn;
		yield return ProcessMapTableNameColumn;

		yield return DescriptionColumn;

		yield return CreateTimestampColumn;
		yield return UpdateTimestampColumn;
	}
}
