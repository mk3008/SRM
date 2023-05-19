namespace InterlinkMapper.System;

public class DbTableConfig
{
	public DbTableDefinition TransactionTable { get; set; } = new();

	public DbTableDefinition ProcessTable { get; set; } = new();

	public DbTableDefinition ProcessResultTable { get; set; } = new();

	public string TransactionIdColumn { get; set; } = string.Empty;

	public string ProcessIdColumn { get; set; } = string.Empty;

	public string DestinationTableNameColumn { get; set; } = string.Empty;

	public string DatasourceNameColumn { get; set; } = string.Empty;

	public string KeymapTableNameColumn { get; set; } = string.Empty;

	public string RelationmapTableNameColumn { get; set; } = string.Empty;

	public string MemberNameColumn { get; set; } = string.Empty;

	public string TableNameColumn { get; set; } = string.Empty;

	public string ActionNameColumn { get; set; } = string.Empty;

	public string ResultCountColumn { get; set; } = string.Empty;

	public string TimestampColumn { get; set; } = string.Empty;
}
