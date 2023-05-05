using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Values;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class ForwardTransferService
{
	public ForwardTransferService(IDbConnection cn, ILogger? logger = null, string placeHolderIdentifer = ":")
	{
		Connection = cn;
		Logger = logger;
		PlaceHolderIdentifer = placeHolderIdentifer;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	private string PlaceHolderIdentifer { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	public int TransferToDestination(IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.Destination.Table);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToKeyMap(IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.KeyMapTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToRelationMap(IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.RelationMapTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		return ExecuteWithLogging(iq);
	}

	public int TransferToRequest(IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.RequestTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		//wait for unnumbered
		var f = sq.FromClause;
		if (f == null) throw new NullReferenceException(nameof(f));
		var r = f.LeftJoin(ds.RequestTable.GetTableFullName()).As("r").On(f.Root, ds.KeyColumns);

		sq.Where(new ColumnValue(f.Root, ds.Destination.Sequence.Column).IsNull());
		sq.Where(new ColumnValue(r, ds.KeyColumns.First()).IsNull());

		return ExecuteWithLogging(iq);
	}

	public int RemoveRequestAsSuccess(IDatasource ds, SelectQuery bridge)
	{
		var requestTable = ds.RequestTable.GetTableFullName();

		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		ds.KeyColumns.ForEach(x => sq.Select(b, x));
		var dq = sq.ToDeleteQuery(requestTable);

		return ExecuteWithLogging(dq);
	}

	public int RemoveRequestAsIgnore(IDatasource ds, SelectQuery bridge, int maxRequestId)
	{
		var requestTable = ds.RequestTable.GetTableFullName();
		var seq = ds.RequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
		var key = ds.KeyColumns.First();

		var sq = new SelectQuery();
		var (f, r) = sq.From(requestTable).As("r");
		var b = f.LeftJoin(bridge).As("b").On(r, ds.KeyColumns);

		sq.Select(r, seq.ColumnName);

		//A request that is not created in the bridge table is an invalid request.
		//However, in order to prevent excessive deletion of requests,
		//the maximum number of requests to be deleted is limited to the request ID used
		//when creating the bridge table.
		sq.Where(b, key).IsNull();
		sq.Where(r, seq.ColumnName).AddOperatableValue("<=", new LiteralValue(sq.AddParameter(PlaceHolderIdentifer + "max_request_id", maxRequestId)));

		var dq = sq.ToDeleteQuery(ds.RequestTable.GetTableFullName());

		return ExecuteWithLogging(dq);
	}

	private static InsertQuery ToInsertQuery(SelectQuery bridge, IDbTable table)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(table.Columns);
		return sq.ToInsertQuery(table.GetTableFullName());
	}

	private int ExecuteWithLogging(IQueryCommandable query)
	{
		var type = (query is InsertQuery) ? "insert" : (query is UpdateQuery) ? "update" : (query is DeleteQuery) ? "delete" : "unknown";
		Logger?.LogInformation("{Type} sql : {Sql}", type, query.ToCommand().CommandText);
		var cnt = Connection.Execute(query, commandTimeout: CommandTimeout);
		Logger?.LogInformation("results : {Count} row(s)", cnt);

		return cnt;
	}
}
