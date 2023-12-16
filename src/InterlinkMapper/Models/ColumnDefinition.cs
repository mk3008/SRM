namespace InterlinkMapper.Models;

public class ColumnDefinition
{
	public string ColumnName { get; set; } = string.Empty;

	public string TypeName { get; set; } = string.Empty;

	public bool AllowNull { get; set; } = false;

	public bool IsPrimaryKey { get; set; } = false;

	public bool IsUniqueKey { get; set; } = false;

	public bool IsAutoNumber { get; set; } = false;

	public string DefaultValue { get; set; } = string.Empty;

	public string Comment { get; set; } = string.Empty;

	public string ToCommandText()
	{
		var name = ColumnName;
		var type = TypeName;
		var sql = $"{name} {type}";

		if (!AllowNull) { sql += " not null"; }
		if (!string.IsNullOrEmpty(DefaultValue)) { sql += " default " + ValueParser.Parse(DefaultValue).ToText(); }

		return sql;
	}
}
