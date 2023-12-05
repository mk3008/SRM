using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterial : MaterializeResult
{
	public required string KeymapTable { get; init; }

	public required List<string> DatasourceKeyColumns { get; init; }

	internal InsertQuery CreateKeymapInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeymapTable);
	}

	internal InsertQuery CreateReverseInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, RootIdColumn);
		sq.Select(d, OriginIdColumn);
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, RemarksColumn);

		sq.Where(d, OriginIdColumn).IsNotNull();

		return sq.ToInsertQuery(ReverseTable);
	}

	public void ExecuteTransfer(IDbConnection connection, long processId)
	{
		// transfer datasource
		connection.Execute(CreateRelationInsertQuery(processId), commandTimeout: CommandTimeout);
		connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);

		// create system relation mapping
		connection.Execute(CreateKeymapInsertQuery(), commandTimeout: CommandTimeout);
		connection.Execute(CreateReverseInsertQuery(), commandTimeout: CommandTimeout);
	}
}
