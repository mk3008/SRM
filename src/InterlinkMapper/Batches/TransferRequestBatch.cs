using Carbunql.Values;
using Carbunql;
using InterlinkMapper.Data;
using System.Data;
using Carbunql.Building;
using Carbunql.Dapper;

namespace InterlinkMapper.Batches;

internal class TransferRequestBatch
{
	public TransferRequestBatch(IDbConnection cn, Database db, Datasource datasource, SelectQuery selectBridgeQuery)
	{
		Connection = cn;
		Datasource = datasource;
		Query = selectBridgeQuery;

		var table = new DbTable()
		{
			TableName = db.ProcessMapNameBuilder(Datasource.Destination),
		};
		table.Columns.Add(db.ProcessIdColumnName);
		table.Columns.Add(datasource.Destination.Sequence.Column);
		ProcessMapTable = table;
	}

	private IDbConnection Connection { get; init; }

	private Datasource Datasource { get; init; }

	private SelectQuery Query { get; init; }

	private DbTable ProcessMapTable { get; init; }

	public int Postpone()
	{
		var table = Datasource.HoldTable;
		var seq = Datasource.Destination.Sequence;

		var sq = new SelectQuery();
		var (_, b) = sq.From(Query).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(table.Columns);
		sq.Where(new ColumnValue(b, seq.Column).IsNull());

		var iq = Query.ToInsertQuery(table.TableFullName);
		return Connection.Execute(iq);
	}

	public int Processed()
	{
		var table = Datasource.HoldTable;
		var seq = Datasource.Destination.Sequence;

		var sq = new SelectQuery();
		var (_, b) = sq.From(Query).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(table.Columns);
		sq.Where(new ColumnValue(b, seq.Column).IsNotNull());

		var dq = sq.ToDeleteQuery(table.TableFullName, Datasource.KeyColumns);
		return Connection.Execute(dq);
	}
}
