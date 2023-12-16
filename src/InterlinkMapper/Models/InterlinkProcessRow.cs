namespace InterlinkMapper.Models;

public class InterlinkProcessRow
{
	public long InterlinkTransactionId { get; set; }

	public long InterlinkProcessId { get; set; }

	public long InterlinkDatasourceId { get; set; }

	public long InterlinkDestinationId { get; set; }

	public required string ActionName { get; set; }

	public long InsertCount { get; set; }

	public required string KeyMapTableName { get; set; }

	public required string KeyRelationTableName { get; set; }
}
