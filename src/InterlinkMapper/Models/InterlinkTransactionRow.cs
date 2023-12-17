namespace InterlinkMapper.Models;

public class InterlinkTransactionRow
{
	public long InterlinkTransactionId { get; set; }

	public long InterlinkDatasourceId { get; set; }

	public long InterlinkDestinationId { get; set; }

	public required string ActionName { get; set; }

	public required string Argument { get; set; } = string.Empty;
}
