using RedOrb;

namespace InterlinkMapper.Models;

public class InterlinkTransactionTable
{
	public required DbTableDefinition Definition { get; set; }

	public required string InterlinkTransactionIdColumn { get; set; }

	public required string InterlinkDatasourceIdColumn { get; set; }

	public required string InterlinkDestinationIdColumn { get; set; }

	public required string ActionNameColumn { get; set; }

	public required string ArgumentColumn { get; set; }
}