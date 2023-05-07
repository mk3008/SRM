using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Extensions;
using Carbunql.Values;
using Cysharp.Text;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper.Services;

/// <summary>
/// 検証リクエストのうち削除依頼のもの
/// </summary>
public class ReverseRequestService
{
	public ReverseRequestService(IDbConnection cn, ILogger? logger = null, string placeHolderIdentifer = ":")
	{
		Connection = cn;
		Logger = logger;
		PlaceHolderIdentifer = placeHolderIdentifer;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	private string PlaceHolderIdentifer { get; init; }

	public int GetLastRequestId(IDatasource ds)
	{
		var requestTable = ds.RequestTable.GetTableFullName();
		if (string.IsNullOrEmpty(requestTable)) return 0;

		var seq = ds.RequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
		var sq = new SelectQuery();
		var (_, r) = sq.From(requestTable).As("r");
		sq.Select($"coalesce(max(r.{seq.ColumnName}), 0)");

		return Connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
	}

	/// <summary>
	/// Create a new bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="injector"></param>
	/// <returns></returns>
	public SelectQuery CreateAndSelect(IDatasource ds, int maxRequestId, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var bridgeName = GenerateBridgeName(ds);

		var sq = new SelectQuery();
		var (_, d) = sq.From(GetSelectDatasourceQueryForTransfer(ds, maxRequestId)).As("d");

		sq.Select(d);

		//assign a Sequence to the transfer target
		var seq = ds.Destination.Sequence;
		if (sq.SelectClause!.Where(x => x.Alias.IsEqualNoCase(ds.HoldJudgementColumnName)).Any())
		{
			sq.Select(seq.Command).As(seq.Column);
		}
		else
		{
			sq.Select(() =>
			{
				var c = new CaseExpression();
				c.When(new ColumnValue(d, ds.HoldJudgementColumnName).False()).Then(new LiteralValue(seq.Command));
				return c;
			}).As(seq.Column);
		}

		//Re-held data is not subject to extraction.
		sq.Where(() => new ColumnValue(d, ds.HoldJudgementColumnName).False());

		if (injector != null) sq = injector(sq);

		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);

		Logger?.LogInformation("create table sql : {Sql}", cq.ToCommand().CommandText);

		Connection.Execute(cq);

		return GetSelectBridgeQuery(bridgeName, columns);
	}

	/// <summary>
	/// Generate a bridge name.
	/// </summary>
	/// <param name="datasource"></param>
	/// <returns></returns>
	private string GenerateBridgeName(IDatasource datasource)
	{
		using MD5 md5Hash = MD5.Create();

		byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(datasource.DatasourceName));
		var sb = ZString.CreateStringBuilder();
		sb.Append("_req_");
		for (int i = 0; i < 4; i++)
		{
			sb.Append(data[i].ToString("x2"));
		}
		return sb.ToString();
	}

	/// <summary>
	/// Returns the number of records.
	/// </summary>
	/// <param name="bridgeQuery"></param>
	/// <returns></returns>
	public int GetCount(SelectQuery bridgeQuery)
	{
		var q = bridgeQuery.ToCountQuery();
		Logger?.LogInformation("count sql : {Sql}", q.ToCommand().CommandText);

		var cnt = Connection.ExecuteScalar<int>(q, commandTimeout: CommandTimeout);
		Logger?.LogInformation("count : {Count} row(s)", cnt);
		return cnt;
	}

	/// <summary>
	/// SELECT columns FROM datasource WHERE not_forwarded
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="keymapTable"></param>
	/// <returns></returns>
	private SelectQuery GetSelectDatasourceQueryForTransfer(IDatasource ds, int maxRequestId)
	{
		var sq = ds.ToSelectDatasourceQuery();

		//In order to fix the target data, narrow it down to the specified request ID or less.
		//This condition is also used when deleting request data.
		AddRequestCondition(ds, sq, maxRequestId);
		return sq;
	}

	/// <summary>
	/// SELECT columns FROM bridgeTable
	/// </summary>
	/// <param name="bridgeTable"></param>
	/// <param name="columns"></param>
	/// <returns></returns>
	private static SelectQuery GetSelectBridgeQuery(string bridgeTable, List<string> columns)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeTable).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}

	private void AddRequestCondition(IDatasource ds, SelectQuery query, int maxRequestId)
	{
		// INNER JOIN request AS r ON d.key1 = m.key1 AND d.key2 = m.key2
		// WHERE r.request_id <= :max_request_id
		var from = query.FromClause;
		if (from == null) throw new NullReferenceException(nameof(from));
		var dsTable = from.Root;

		var requestTable = ds.RequestTable.GetTableFullName();
		if (string.IsNullOrEmpty(requestTable)) throw new NullReferenceException(nameof(requestTable));

		var r = from.InnerJoin(requestTable).As("r").On(dsTable, ds.KeyColumns);

		var seq = ds.RequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();
		query.Where(r, seq.ColumnName).AddOperatableValue("<=", new LiteralValue(query.AddParameter(PlaceHolderIdentifer + "max_request_id", maxRequestId)));
	}
}
