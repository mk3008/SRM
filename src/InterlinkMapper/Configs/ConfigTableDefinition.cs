using InterlinkMapper.Data;

namespace InterlinkMapper.Configs;

public class ConfigTableDefinition : ITableDefinition
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = "configs";

	public ColumnDefinition IdColumn { get; set; } = new ColumnDefinition() { ColumnName = "config_id", TypeName = "serial8", IsPrimaryKey = true, IsAutoNumber = true };

	public ColumnDefinition NameColumn { get; set; } = new ColumnDefinition() { ColumnName = "config_name", TypeName = "text", IsUniqueKey = true };

	public ColumnDefinition CreateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public ColumnDefinition UpdateTimestampColumn { get; set; } = new ColumnDefinition() { ColumnName = "updated_at", TypeName = "timestmap", DefaultValue = "current_timestamp" };

	public IEnumerable<ColumnDefinition> GetColumns()
	{
		yield return IdColumn;
		yield return NameColumn;

		yield return CreateTimestampColumn;
		yield return UpdateTimestampColumn;
	}
}
