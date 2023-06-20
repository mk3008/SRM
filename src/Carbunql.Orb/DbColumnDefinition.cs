using Carbunql.Analysis.Parser;

namespace Carbunql.Orb;

public class DbColumnDefinition
{
	public string Identifer { get; set; } = string.Empty;

	public required string ColumnName { get; set; }

	public required string TypeName { get; set; }

	public bool AllowNull { get; set; } = false;

	public bool IsPrimaryKey { get; set; } = false;

	public bool IsUniqueKey { get; set; } = false;

	public bool IsAutoNumber { get; set; } = false;

	public string DefaultValue { get; set; } = string.Empty;

	public string Comment { get; set; } = string.Empty;

	public SpecialColumn SpecialColumn { get; set; } = SpecialColumn.None;

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

public enum SpecialColumn
{
	None,
	CreateTimestamp,
	UpdateTimestamp,
	VersionNumber,
}
