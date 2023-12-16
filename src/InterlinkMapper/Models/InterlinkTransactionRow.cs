namespace InterlinkMapper.Models;

public class InterlinkTransactionRow
{
	public long InterlinkTransactionId { get; set; }

	public long InterlinkDatasourceId { get; set; }

	public long InterlinkDestinationId { get; set; }

	public string Argument { get; set; } = string.Empty;
}
