﻿namespace InterlinkMapper.Models;

public class InsertRequestTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RequestIdColumn { get; set; } = string.Empty;

	public List<string> DatasourceKeyColumns { get; set; } = new();
}