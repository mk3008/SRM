namespace InterlinkMapper.Materializer;

public abstract class MaterializeResult
{
	public required SelectQuery SelectQuery { get; init; } = null!;

	public required int Count { get; init; }

	public required string MaterialName { get; init; } = string.Empty;

	public required string PlaceHolderIdentifer { get; init; }

	public required string ProcessIdColumn { get; init; }

	public required string DestinationTable { get; init; }

	public required string DestinationIdColumn { get; init; }

	public required List<string> DestinationColumns { get; init; }

	public required string RelationTable { get; init; }

	public required string RootIdColumn { get; init; }

	public required string OriginIdColumn { get; init; }

	public required string RemarksColumn { get; init; }

	public required string ReverseTable { get; init; }

	public required int CommandTimeout { get; init; }

	internal InsertQuery CreateRelationInsertQuery(long processId)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		sq.Select(PlaceHolderIdentifer, ProcessIdColumn, processId);

		return sq.ToInsertQuery(RelationTable);
	}

	internal InsertQuery CreateDestinationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(DestinationColumns);

		return sq.ToInsertQuery(DestinationTable);
	}
}