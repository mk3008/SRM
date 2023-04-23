using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Values;
using Dapper;
using InterlinkMapper.Data;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class ForwardTransferService
{
	public ForwardTransferService(IDbConnection cn, ILogger? logger = null)
	{
		Connection = cn;
		Logger = logger;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }


	public int TransferToDestination(Datasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.Destination.Table);
		var sq = iq.Query as SelectQuery;
		if (sq == null) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToKeyMap(Datasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.KeyMapTable);
		var sq = iq.Query as SelectQuery;
		if (sq == null) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToRelationMap(Datasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.RelationMapTable);
		var sq = iq.Query as SelectQuery;
		if (sq == null) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToHold(Datasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.HoldTable);
		var sq = iq.Query as SelectQuery;
		if (sq == null) throw new NullReferenceException(nameof(sq));

		//wait for unnumbered
		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNull());

		return ExecuteWithLogging(iq);
	}

	public int RemoveRequest(Datasource ds, SelectQuery bridge)
	{
		var sq = new SelectQuery();
		var (f, b) = sq.From(bridge).As("b");
		sq.Select(b);
		var dq = sq.ToDeleteQuery(ds.RequestTable.GetTableFullName(), ds.KeyColumns);

		return ExecuteWithLogging(dq);
	}

	private InsertQuery ToInsertQuery(SelectQuery bridge, IDbTable table)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(table.Columns);
		return sq.ToInsertQuery(table.GetTableFullName());
	}

	private int ExecuteWithLogging(IQueryCommandable query)
	{
		Logger?.LogInformation(query.ToCommand().CommandText);
		var cnt = Connection.Execute(query);
		Logger?.LogInformation($"count : {cnt} row(s)");

		return cnt;
	}
}
