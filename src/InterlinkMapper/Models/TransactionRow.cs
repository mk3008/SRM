namespace InterlinkMapper.Models;

public class TransactionRow
{
	public long TransactionId { get; set; }

	public long DatasourceId { get; set; }

	public long DestinationId { get; set; }

	public string Argument { get; set; } = string.Empty;
}
