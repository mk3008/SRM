using RedOrb;

namespace InterlinkMapper.Models;

public class InterlinkRelationTable
{
	public required DbTableDefinition Definition { get; init; }

	public required string InterlinkProcessIdColumn { get; init; }

	public required string DestinationIdColumn { get; init; }

	public required string RootIdColumn { get; init; }

	public required string OriginIdColumn { get; init; }

	public required string RemarksColumn { get; init; }
}
