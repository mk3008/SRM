namespace InterlinkMapper.Models;

public class TransactionTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string TransactionIdColumn { get; set; } = string.Empty;

	public string DatasourceIdColumn { get; set; } = string.Empty;

	public string DestinationIdColumn { get; set; } = string.Empty;

	public string ActionColumn { get; set; } = string.Empty;

	public string ArgumentColumn { get; set; } = string.Empty;
}