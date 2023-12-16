namespace InterlinkMapper.Models;

public class InterlinkProcessTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string InterlinkTransactionIdColumn { get; set; } = string.Empty;

	public string InterlinkProcessIdColumn { get; set; } = string.Empty;

	public string InterlinkDatasourceIdColumn { get; set; } = string.Empty;

	public string InterlinkDestinationIdColumn { get; set; } = string.Empty;

	public string ActionColumn { get; set; } = string.Empty;

	public string InsertCountColumn { get; set; } = string.Empty;

	public string KeyMapTableNameColumn { get; set; } = string.Empty;

	public string KeyRelationTableNameColumn { get; set; } = string.Empty;
}
