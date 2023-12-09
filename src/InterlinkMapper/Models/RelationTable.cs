namespace InterlinkMapper.Models;

public class RelationTable
{
	public required DbTableDefinition Definition { get; init; }

	public required string ProcessIdColumn { get; init; }

	public required string DestinationIdColumn { get; init; }

	public required string RootIdColumn { get; init; }

	public required string OriginIdColumn { get; init; }
}
