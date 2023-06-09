﻿using Carbunql.Analysis.Parser;

namespace Carbunql.Orb;

public class DbColumnDefinition
{
	public string Identifer { get; set; } = string.Empty;

	public required string ColumnName { get; set; }

	public required string ColumnType { get; set; }

	public bool IsNullable { get; set; } = false;

	public bool IsPrimaryKey { get; set; } = false;

	public bool IsUniqueKey { get; set; } = false;

	public bool IsAutoNumber { get; set; } = false;

	public string DefaultValue { get; set; } = string.Empty;

	public string Comment { get; set; } = string.Empty;

	//public Type? RelationType { get; set; }

	public SpecialColumn SpecialColumn { get; set; } = SpecialColumn.None;

	public string ToCommandText()
	{
		var name = ColumnName;
		var type = ColumnType;
		var sql = $"{name} {type}";

		if (!IsNullable) { sql += " not null"; }
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
