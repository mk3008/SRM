namespace InterlinkMapper.Models;

public class ProcessRow
{
	public long TransactionId { get; set; }

	public long ProcessId { get; set; }

	public long DatasourceId { get; set; }

	public long DestinationId { get; set; }

	public string ActionName { get; set; } = string.Empty;

	public long InsertCount { get; set; }

	public string KeyMapTableName { get; set; } = string.Empty;

	public string KeyRelationTableName { get; set; } = string.Empty;
}
