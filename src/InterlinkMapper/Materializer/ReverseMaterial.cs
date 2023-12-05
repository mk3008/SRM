using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseMaterial : MaterializeResult
{
	public required string KeymapTableNameColumn { get; init; }

	internal SelectQuery CreateKeymapTableNameSelectQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, KeymapTableNameColumn);
		sq.SelectClause!.HasDistinctKeyword = true;

		return sq;
	}

	internal DeleteQuery CreateKeymapDeleteQuery(string keymapTable)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, OriginIdColumn).As(DestinationIdColumn);

		var q = sq.ToDeleteQuery(keymapTable);
		q.AddComment("canceling the keymap due to reverse");

		return q;
	}

	internal InsertQuery CreateReverseInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, RootIdColumn);
		sq.Select(d, OriginIdColumn);
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, RemarksColumn);

		return sq.ToInsertQuery(ReverseTable);
	}

	internal void ExecuteTransfer(IDbConnection connection, long processId)
	{
		// transfer datasource
		connection.Execute(CreateRelationInsertQuery(processId), commandTimeout: CommandTimeout);
		connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);

		// create system relation mapping
		var keymaps = connection.Query<string>(CreateKeymapTableNameSelectQuery(), commandTimeout: CommandTimeout).ToList();
		foreach (var keymap in keymaps)
		{
			connection.Execute(CreateKeymapDeleteQuery(keymap), commandTimeout: CommandTimeout);
		}
		connection.Execute(CreateReverseInsertQuery(), commandTimeout: CommandTimeout);
	}
}
