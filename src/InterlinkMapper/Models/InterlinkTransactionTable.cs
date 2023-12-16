namespace InterlinkMapper.Models;

public class InterlinkTransactionTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string InterlinkTransactionIdColumn { get; set; } = string.Empty;

	public string InterlinkDatasourceIdColumn { get; set; } = string.Empty;

	public string InterlinkDestinationIdColumn { get; set; } = string.Empty;

	public string ActionColumn { get; set; } = string.Empty;

	public string ArgumentColumn { get; set; } = string.Empty;
}