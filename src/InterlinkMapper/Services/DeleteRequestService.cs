using Dapper;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

/// <summary>
/// 検証リクエストのうち削除依頼でないもの
/// </summary>
public class DeleteRequestService : IQueryExecuteService
{
	public DeleteRequestService(SystemEnvironment environment, IDbConnection cn, int processId, ILogger? logger = null)
	{
		Environment = environment;
		Connection = cn;
		ProcessId = processId;
		Logger = logger;
	}

	public ILogger? Logger { get; init; }

	public IDbConnection Connection { get; init; }

	public SystemEnvironment Environment { get; init; }

	private DbEnvironment DbQueryConfig => Environment.DbEnvironment;

	private DbTableConfig DbTableConfig => Environment.DbTableConfig;

	public int ProcessId { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	public int GetLastRequestId(IDestination ds)
	{
		var requestTable = ds.DeleteRequestTable;
		if (string.IsNullOrEmpty(requestTable.GetTableFullName())) return 0;

		var seq = requestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
		var sq = new SelectQuery();
		var (_, r) = sq.From(requestTable.GetTableFullName()).As("r");
		sq.Select($"coalesce(max(r.{seq.ColumnName}), 0)");

		return Connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
	}

	public List<string> GetKeymapTableNames(IDestination ds, int maxRequestId)
	{
		var sq = GenerateSelectQueryAsKeymapTableName(ds, maxRequestId);
		var lst = Connection.Query<string>(sq, commandTimeout: CommandTimeout).ToList();
		return lst;
	}

	public List<string> GetRelationmapTableNames(IDestination ds, int maxRequestId)
	{
		var sq = GenerateSelectQueryAsRelationmapTableName(ds, maxRequestId);
		var lst = Connection.Query<string>(sq, commandTimeout: CommandTimeout).ToList();
		return lst;
	}

	public int DeleteKeymap(IDestination ds, int maxRequestId, string keymapTable)
	{
		var sq = GenerateSelectQueryForMapping(ds, maxRequestId);
		var cnt = this.Delete(sq, keymapTable);
		return cnt;
	}

	public int DeleteRelationMap(IDestination ds, int maxRequestId, string relationmapTable)
	{
		var sq = GenerateSelectQueryForMapping(ds, maxRequestId);
		var cnt = this.Delete(sq, relationmapTable);
		return cnt;
	}

	public int DeleteDestination(IDestination ds, int maxRequestId)
	{
		var sq = GenerateSelectQueryForMapping(ds, maxRequestId);
		var cnt = this.Delete(sq, ds.Table);
		return cnt;
	}

	public int DeleteRequest(IDestination ds, int maxRequestId)
	{
		var sq = GenerateSelectQueryForMapping(ds, maxRequestId);
		var cnt = this.Delete(sq, ds.DeleteRequestTable.GetTableFullName());
		return cnt;
	}

	private SelectQuery GenerateSelectQueryAsKeymapTableName(IDestination ds, int maxRequestId)
	{
		var seq = ds.Sequence.Column;
		var key = DbTableConfig.ProcessIdColumn;
		var reqIdColumn = ds.DeleteRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

		var sq = new SelectQuery();
		var (f, r) = sq.From(ds.DeleteRequestTable.GetTableFullName()).As("r");
		var dp = f.InnerJoin(ds.ProcessTable.Definition.GetTableFullName()).As("dp").On(r, seq);
		var p = f.InnerJoin(DbTableConfig.ProcessTable.GetTableFullName()).As("p").On(dp, key);

		var pname = sq.AddParameter(DbQueryConfig.PlaceHolderIdentifer + "max_request_id", maxRequestId);
		sq.Where(r, reqIdColumn.ColumnName).AddOperatableValue("<=", new LiteralValue(pname));

		sq.Select(p, DbTableConfig.KeymapTableNameColumn);
		sq.SelectClause!.HasDistinctKeyword = true;

		return sq;
	}

	private SelectQuery GenerateSelectQueryAsRelationmapTableName(IDestination ds, int maxRequestId)
	{
		var seq = ds.Sequence.Column;
		var key = DbTableConfig.ProcessIdColumn;
		var reqIdColumn = ds.DeleteRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

		var sq = new SelectQuery();
		var (f, r) = sq.From(ds.DeleteRequestTable.GetTableFullName()).As("r");
		var dp = f.InnerJoin(ds.ProcessTable.Definition.GetTableFullName()).As("dp").On(r, seq);
		var p = f.InnerJoin(DbTableConfig.ProcessTable.GetTableFullName()).As("p").On(dp, key);

		var pname = sq.AddParameter(DbQueryConfig.PlaceHolderIdentifer + "max_request_id", maxRequestId);
		sq.Where(r, reqIdColumn.ColumnName).AddOperatableValue("<=", new LiteralValue(pname));

		sq.Select(p, DbTableConfig.RelationmapTableNameColumn);
		sq.SelectClause!.HasDistinctKeyword = true;

		return sq;
	}

	private SelectQuery GenerateSelectQueryForMapping(IDestination ds, int maxRequestId)
	{
		var seq = ds.Sequence.Column;
		var key = DbTableConfig.ProcessIdColumn;
		var reqIdColumn = ds.DeleteRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

		var sq = new SelectQuery();
		var (f, r) = sq.From(ds.DeleteRequestTable.GetTableFullName()).As("r");
		var dp = f.InnerJoin(ds.ProcessTable.Definition.GetTableFullName()).As("dp").On(r, seq);
		var p = f.InnerJoin(DbTableConfig.ProcessTable.GetTableFullName()).As("p").On(dp, key);

		var pReq = sq.AddParameter(DbQueryConfig.PlaceHolderIdentifer + "max_request_id", maxRequestId);
		sq.Where(r, reqIdColumn.ColumnName).AddOperatableValue("<=", new LiteralValue(pReq));

		sq.Select(r, seq);

		return sq;
	}

}
