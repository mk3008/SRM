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
	public ForwardTransferService(DbEnvironment environment, IDbConnection cn, ILogger? logger = null)
	{
		Environment = environment;
		Connection = cn;
		Logger = logger;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	private DbEnvironment Environment { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	public int TransferToDestination(int procId, IDatasource ds, SelectQuery bridge)
	{
		TransferToProcessMap(procId, ds, bridge);

		var iq = ToInsertQuery(bridge, ds.Destination.Table);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		var cnt = ExecuteWithLogging(iq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.Destination.Table.GetTableFullName(), QueryAction.Insert, cnt);

		return cnt;
	}

	private void TransferToProcessMap(int procId, IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(procId, bridge, ds.Destination.ProcessTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		var cnt = ExecuteWithLogging(iq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.Destination.ProcessTable.GetTableFullName(), QueryAction.Insert, cnt);
	}

	public int TransferToKeyMap(int procId, IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.KeyMapTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		var cnt = ExecuteWithLogging(iq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.KeyMapTable.GetTableFullName(), QueryAction.Insert, cnt);

		return cnt;
	}

	public int TransferToRelationMap(int procId, IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.RelationMapTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		sq.Where(new ColumnValue(sq.FromClause!.Root, ds.Destination.Sequence.Column).IsNotNull());

		var cnt = ExecuteWithLogging(iq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.RelationMapTable.GetTableFullName(), QueryAction.Insert, cnt);

		return cnt;
	}

	public int TransferToRequest(int procId, IDatasource ds, SelectQuery bridge)
	{
		var iq = ToInsertQuery(bridge, ds.ForwardRequestTable);
		if (iq.Query is not SelectQuery sq) throw new NullReferenceException(nameof(sq));

		//wait for unnumbered
		var f = sq.FromClause;
		if (f == null) throw new NullReferenceException(nameof(f));
		var r = f.LeftJoin(ds.ForwardRequestTable.GetTableFullName()).As("r").On(f.Root, ds.KeyColumns);

		sq.Where(new ColumnValue(f.Root, ds.Destination.Sequence.Column).IsNull());
		sq.Where(new ColumnValue(r, ds.KeyColumns.First()).IsNull());

		var cnt = ExecuteWithLogging(iq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.ForwardRequestTable.GetTableFullName(), QueryAction.Insert, cnt);

		return cnt;
	}

	public int RemoveRequestAsSuccess(int procId, IDatasource ds, SelectQuery bridge)
	{
		var requestTable = ds.ForwardRequestTable.GetTableFullName();

		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		ds.KeyColumns.ForEach(x => sq.Select(b, x));
		var dq = sq.ToDeleteQuery(requestTable);

		var cnt = ExecuteWithLogging(dq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, requestTable, QueryAction.Delete, cnt);

		return cnt;
	}

	public int RemoveRequestAsIgnore(int procId, IDatasource ds, SelectQuery bridge, int maxRequestId)
	{
		var requestTable = ds.ForwardRequestTable.GetTableFullName();
		var seq = ds.ForwardRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
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
		sq.Where(r, seq.ColumnName).AddOperatableValue("<=", new LiteralValue(sq.AddParameter(Environment.PlaceHolderIdentifer + "max_request_id", maxRequestId)));

		var dq = sq.ToDeleteQuery(ds.ForwardRequestTable.GetTableFullName());

		var cnt = ExecuteWithLogging(dq);

		SaveResult(procId, System.Reflection.MethodBase.GetCurrentMethod()?.Name, ds.ForwardRequestTable.GetTableFullName(), QueryAction.Delete, cnt);

		return cnt;
	}

	private static InsertQuery ToInsertQuery(SelectQuery bridge, IDbTable table)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(table.Columns);
		return sq.ToInsertQuery(table.GetTableFullName());
	}

	private InsertQuery ToInsertQuery(int procId, SelectQuery bridge, IDbTable table)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.Select(sq.AddParameter(Environment.PlaceHolderIdentifer + Environment.ProcessIdColumn, procId)).As(Environment.ProcessIdColumn);
		sq.SelectClause!.FilterInColumns(table.Columns);
		return sq.ToInsertQuery(table.GetTableFullName());
	}

	private int ExecuteWithLogging(IQueryCommandable query)
	{
		Logger?.LogInformation(query.ToText() + ";");
		var cnt = Connection.Execute(query, commandTimeout: CommandTimeout);
		Logger?.LogInformation("results : {cnt} row(s)", cnt);

		return cnt;
	}

	private void SaveResult(int procId, string? functionName, string table, QueryAction action, int count)
	{
		var name = (functionName ?? "unknown");
		var e = Environment;
		var sq = new SelectQuery();
		sq.Select(sq.AddParameter(e.PlaceHolderIdentifer + e.ProcessIdColumn, procId)).As(e.ProcessIdColumn);
		sq.Select(sq.AddParameter(e.PlaceHolderIdentifer + e.FunctionNameColumn, name)).As(e.FunctionNameColumn);
		sq.Select(sq.AddParameter(e.PlaceHolderIdentifer + e.TableNameColumn, table)).As(e.TableNameColumn);
		sq.Select(sq.AddParameter(e.PlaceHolderIdentifer + e.ActionNameColumn, action.ToString())).As(e.ActionNameColumn);
		sq.Select(sq.AddParameter(e.PlaceHolderIdentifer + e.ResultCountColumn, count)).As(e.ResultCountColumn);

		var iq = sq.ToInsertQuery(e.ProcessResultTable.GetTableFullName());
		ExecuteWithLogging(iq);
	}
}

public enum QueryAction
{
	Insert,
	Update,
	Delete,
}
