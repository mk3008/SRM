namespace InterlinkMapper.Models;

public class InterlinkProcessRow
{
	public long InterlinkTransactionId { get; set; }

	public long InterlinkProcessId { get; set; }

	public long InterlinkDatasourceId { get; set; }

	public long InterlinkDestinationId { get; set; }

	public string ActionName { get; set; } = string.Empty;

	public long InsertCount { get; set; }

	public string KeyMapTableName { get; set; } = string.Empty;

	public string KeyRelationTableName { get; set; } = string.Empty;
}
