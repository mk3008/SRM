﻿namespace InterlinkMapper.Models;

public class RelationTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string ProcessIdColumn { get; set; } = string.Empty;

	public string DestinationSequenceColumn { get; set; } = string.Empty;
}
