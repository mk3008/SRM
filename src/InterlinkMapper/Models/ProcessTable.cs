﻿namespace InterlinkMapper.Models;

public class ProcessTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string TransactionIdColumn { get; set; } = string.Empty;

	public string ProcessIdColumn { get; set; } = string.Empty;

	public string DatasourceIdColumn { get; set; } = string.Empty;

	public string DestinationIdColumn { get; set; } = string.Empty;

	public string ActionColumn { get; set; } = string.Empty;

	public string InsertCountColumn { get; set; } = string.Empty;

	public string KeymapTableNameColumn { get; set; } = string.Empty;
	//public string FlipFlagColumnName { get; set; } = string.Empty;
}
