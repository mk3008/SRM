using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseMaterial : MaterializeResult
{
	internal SelectQuery CreateProcessRowSelectQuery(long transactionId)
	{
		var sq = new SelectQuery();
		var (f, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, InterlinkDatasourceIdColumn).As(nameof(InterlinkProcessRow.InterlinkDatasourceId));
		sq.Select(d, InterlinkDestinationIdColumn).As(nameof(InterlinkProcessRow.InterlinkDestinationId));
		sq.Select(d, KeyMapTableNameColumn).As(nameof(InterlinkProcessRow.KeyMapTableName));
		sq.Select(d, KeyRelationTableNameColumn).As(nameof(InterlinkProcessRow.KeyRelationTableName));
		sq.GetSelectableItems().ToList().ForEach(sq.Group);
		sq.GetSelectableItems().ToList().ForEach(x => sq.Order(x.Value));

		sq.Select(PlaceHolderIdentifer, nameof(InterlinkProcessRow.InterlinkTransactionId), transactionId);
		sq.Select(PlaceHolderIdentifer, nameof(InterlinkProcessRow.ActionName), "reverse");

		sq.Select(new FunctionValue("count", "*")).As(nameof(InterlinkProcessRow.InsertCount));

		return sq;
	}

	internal void ExecuteTransfer(IDbConnection connection, long transactionId)
	{
		var rows = connection.Query<InterlinkProcessRow>(CreateProcessRowSelectQuery(transactionId), commandTimeout: CommandTimeout).ToList();

		foreach (var row in rows)
		{
			// regist process
			row.InterlinkProcessId = connection.ExecuteScalar<long>(CreateProcessInsertQuery(row));

			// transfer datasource
			var cnt = connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);
			if (cnt != row.InsertCount) throw new InvalidOperationException();

			// create system relation mapping
			cnt = connection.Execute(CreateMapDeleteQuery(row), commandTimeout: CommandTimeout);
			if (cnt != row.InsertCount) throw new InvalidOperationException();

			cnt = connection.Execute(CreateRelationInsertQuery(row), commandTimeout: CommandTimeout);
			if (cnt != row.InsertCount) throw new InvalidOperationException();
		}
	}

	private InsertQuery CreateRelationInsertQuery(InterlinkProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(row)).As("d");

		sq.Select(PlaceHolderIdentifer, InterlinkProcessIdColumn, row.InterlinkProcessId);
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, RootIdColumn);
		sq.Select(d, OriginIdColumn);
		sq.Select(d, InterlinkRemarksColumn);

		sq.Order(d, DestinationIdColumn);

		return sq.ToInsertQuery(row.KeyRelationTableName);
	}

	private DeleteQuery CreateMapDeleteQuery(InterlinkProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(row)).As("d");

		sq.Select(d, OriginIdColumn).As(DestinationIdColumn);

		return sq.ToDeleteQuery(row.KeyMapTableName);
	}

	private SelectQuery CreateMaterialSelectQuery(InterlinkProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		sq.Where(d, InterlinkDatasourceIdColumn).Equal(new ParameterValue(PlaceHolderIdentifer + InterlinkDatasourceIdColumn, row.InterlinkDatasourceId));
		sq.Where(d, InterlinkDestinationIdColumn).Equal(new ParameterValue(PlaceHolderIdentifer + InterlinkDestinationIdColumn, row.InterlinkDestinationId));
		sq.Where(d, KeyMapTableNameColumn).Equal(new ParameterValue(PlaceHolderIdentifer + KeyMapTableNameColumn, row.KeyMapTableName));
		sq.Where(d, KeyRelationTableNameColumn).Equal(new ParameterValue(PlaceHolderIdentifer + KeyRelationTableNameColumn, row.KeyRelationTableName));

		sq.Select(d);

		return sq;
	}
}

[GeneratePrivateProxy(typeof(ReverseMaterial))]
public partial struct ReverseMaterialProxy;