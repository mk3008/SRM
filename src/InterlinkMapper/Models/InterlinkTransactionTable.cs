using RedOrb;

namespace InterlinkMapper.Models;

[Obsolete]
public class InterlinkTransactionTable
{
	public required DbTableDefinition Definition { get; set; }

	public required string InterlinkTransactionIdColumn { get; set; }

	public required string InterlinkDestinationIdColumn { get; set; }

	public required string ServiceNameColumn { get; set; }

	public required string ArgumentColumn { get; set; }
}