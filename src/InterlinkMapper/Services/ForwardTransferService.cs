using Carbunql;
using Carbunql.Building;
using Carbunql.Clauses;
using Carbunql.Values;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class ForwardTransferService : IQueryExecuteService
{
	public ForwardTransferService(SystemEnvironment environment, IDbConnection cn, int processId, ILogger? logger = null)
	{
		Environment = environment;
		Connection = cn;
		ProcessId = processId;
		Logger = logger;
	}

	public ILogger? Logger { get; init; }

	public IDbConnection Connection { get; init; }

	public SystemEnvironment Environment { get; init; }

	private DbQueryConfig DbQueryConfig => Environment.DbQueryConfig;

	private DbTableConfig DbTableConfig => Environment.DbTableConfig;

	public int ProcessId { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	public int TransferToDestination(IDatasource ds, SelectQuery bridge)
	{
		TransferToProcessMap(ds, bridge);

		var sq = GenerateSelectQueryFromBridgeWhereForwardable(ds, bridge);
		var cnt = this.Insert(sq, ds.Destination.Table);
		return cnt;
	}

	private int TransferToProcessMap(IDatasource ds, SelectQuery bridge)
	{
		var sq = GenerateSelectQueryFromBridgeWhereForwardable(ds, bridge);
		var cnt = this.Insert(sq, ds.Destination.ProcessTable.Definition);
		return cnt;
	}

	public int TransferToKeyMap(IDatasource ds, SelectQuery bridge)
	{
		var sq = GenerateSelectQueryFromBridgeWhereForwardable(ds, bridge);
		var cnt = this.Insert(sq, ds.KeyMapTable);
		return cnt;
	}

	public int TransferToRelationMap(IDatasource ds, SelectQuery bridge)
	{
		var sq = GenerateSelectQueryFromBridgeWhereForwardable(ds, bridge);
		var cnt = this.Insert(sq, ds.RelationMapTable);
		return cnt;
	}

	public int TransferToRequestAsHold(IDatasource ds, SelectQuery bridge)
	{
		var sq = GenerateSelectQueryFromBridgeWhereNotForwardable(ds, bridge);
		AddConditionAsNotExistInForwardRequest(ds, sq);
		var cnt = this.Insert(sq, ds.ForwardRequestTable);
		return cnt;
	}

	private void AddConditionAsNotExistInForwardRequest(IDatasource ds, SelectQuery sq)
	{
		var d = sq.FromClause!.Root;
		var r = sq.FromClause!.LeftJoin(ds.ForwardRequestTable.GetTableFullName()).As("r").On(d, ds.KeyColumns);
		sq.Where(r, ds.KeyColumns.First()).IsNull();
	}

	public int DeleteRequestAsSuccess(IDatasource ds, SelectQuery bridge)
	{
		var sq = GenerateSelectQueryFromBridgeWhereForwardable(ds, bridge);
		sq.SelectClause!.FilterInColumns(ds.KeyColumns);
		var cnt = this.Delete(sq, ds.ForwardRequestTable);
		return cnt;
	}

	public int DeleteRequestAsIgnore(IDatasource ds, SelectQuery bridge, int maxRequestId)
	{
		var sq = GenerateSelectQueryFromForwardRequestAsIgnore(ds, bridge, maxRequestId);
		var cnt = this.Delete(sq, ds.ForwardRequestTable);
		return cnt;
	}

	private SelectQuery GenerateSelectQueryFromForwardRequestAsIgnore(IDatasource ds, SelectQuery bridge, int maxRequestId)
	{
		var seq = ds.ForwardRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
		var key = ds.KeyColumns.First();

		var sq = new SelectQuery();
		var (f, r) = sq.From(ds.ForwardRequestTable.GetTableFullName()).As("r");
		var b = f.LeftJoin(bridge).As("b").On(r, ds.KeyColumns);
		sq.Where(b, key).IsNull();

		var pname = sq.AddParameter(DbQueryConfig.PlaceHolderIdentifer + "max_request_id", maxRequestId);
		sq.Where(r, seq.ColumnName).AddOperatableValue("<=", new LiteralValue(pname));

		sq.Select(r, seq.ColumnName);

		return sq;
	}

	private SelectQuery GenerateSelectQueryFromBridgeWhereForwardable(IDatasource ds, SelectQuery bridge)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge).As("d");
		sq.Select(d);
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, "process_id", ProcessId);
		sq.Select(d, ds.Destination.Sequence.Column).As(ds.Destination.ProcessTable.RootIdColumnName);
		sq.Select("false").As(ds.Destination.ProcessTable.FlipColumnName);

		sq.Where(d, ds.Destination.Sequence.Column).IsNotNull();
		return sq;
	}

	private SelectQuery GenerateSelectQueryFromBridgeWhereNotForwardable(IDatasource ds, SelectQuery bridge)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge).As("d");
		sq.Select(d);
		sq.Where(d, ds.Destination.Sequence.Column).IsNull();
		return sq;
	}
}
