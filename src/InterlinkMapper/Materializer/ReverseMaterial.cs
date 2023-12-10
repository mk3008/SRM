using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseMaterial : MaterializeResult
{
	//public required string KeyMapColumn { get; init; }

	//public required string KeyRelationColumn { get; init; }



	internal SelectQuery CreateProcessRowSelectQuery(long transactionId)
	{
		var sq = new SelectQuery();
		var (f, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, ProcessDatasourceIdColumn).As(nameof(ProcessRow.DatasourceId));
		sq.Select(d, ProcessDestinationIdColumn).As(nameof(ProcessRow.DestinationId));
		sq.Select(d, KeyMapTableNameColumn).As(nameof(ProcessRow.KeyMapTableName));
		sq.Select(d, KeyRelationTableNameColumn).As(nameof(ProcessRow.KeyRelationTableName));
		sq.GetSelectableItems().ToList().ForEach(sq.Group);
		sq.GetSelectableItems().ToList().ForEach(x => sq.Order(x.Value));

		sq.Select(PlaceHolderIdentifer, nameof(ProcessRow.TransactionId), transactionId);

		sq.Select(new FunctionValue("count", "*")).As(nameof(ProcessRow.InsertCount));

		return sq;
	}

	internal void ExecuteTransfer(IDbConnection connection, long transactionId)
	{
		var rows = connection.Query<ProcessRow>(CreateProcessRowSelectQuery(transactionId), commandTimeout: CommandTimeout).ToList();

		foreach (var row in rows)
		{
			// regist process
			row.ProcessId = connection.ExecuteScalar<long>(CreateProcessInsertQuery(row));

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

	private InsertQuery CreateRelationInsertQuery(ProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(row)).As("d");

		sq.Select(PlaceHolderIdentifer, ProcessIdColumn, row.ProcessId);
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, RootIdColumn);
		sq.Select(d, OriginIdColumn);
		sq.Select(d, RemarksColumn);

		sq.Order(d, DestinationIdColumn);

		return sq.ToInsertQuery(row.KeyRelationTableName);
	}

	private DeleteQuery CreateMapDeleteQuery(ProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(row)).As("d");

		sq.Select(d, OriginIdColumn).As(DestinationIdColumn);

		return sq.ToDeleteQuery(row.KeyMapTableName);
	}

	private SelectQuery CreateMaterialSelectQuery(ProcessRow row)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		sq.Where(d, ProcessDatasourceIdColumn).Equal(new ParameterValue(PlaceHolderIdentifer + ProcessDatasourceIdColumn, row.DatasourceId));
		sq.Where(d, ProcessDestinationIdColumn).Equal(new ParameterValue(PlaceHolderIdentifer + ProcessDestinationIdColumn, row.DestinationId));
		sq.Where(d, KeyMapTableNameColumn).Equal(new ParameterValue(PlaceHolderIdentifer + KeyMapTableNameColumn, row.KeyMapTableName));
		sq.Where(d, KeyRelationTableNameColumn).Equal(new ParameterValue(PlaceHolderIdentifer + KeyRelationTableNameColumn, row.KeyRelationTableName));

		sq.Select(d);

		return sq;
	}
}

[GeneratePrivateProxy(typeof(ReverseMaterial))]
public partial struct ReverseMaterialProxy;