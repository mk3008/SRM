using Carbunql;
using Carbunql.Building;
using Carbunql.Clauses;
using Carbunql.Dapper;
using Carbunql.Extensions;
using Carbunql.Values;
using Cysharp.Text;
using Dapper;
using InterlinkMapper.Data;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper.Services;

public class HoldBridgeService
{
	public HoldBridgeService(IDbConnection cn, ILogger? logger = null, string holdJudgementColumnName = "_hold", string transferJudgementColumnName = "_target")
	{
		Connection = cn;
		Logger = logger;
		HoldJudgementColumnName = holdJudgementColumnName;
		TransferJudgementColumnName = transferJudgementColumnName;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	private string HoldJudgementColumnName { get; init; }

	private string TransferJudgementColumnName { get; init; }

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
		sb.Append("_hld_");
		for (int i = 0; i < 4; i++)
		{
			sb.Append(data[i].ToString("x2"));
		}
		return sb.ToString();
	}

	/// <summary>
	/// Create a new bridge table.
	/// </summary>
	/// <param name="datasource"></param>
	/// <param name="injector"></param>
	/// <returns></returns>
	public SelectQuery CreateAsNew(IDatasource datasource, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var bridgeName = GenerateBridgeName(datasource);
		var keymapTable = datasource.KeyMapTable.TableFullName;

		var q = GetFilteredDatasourceQuery(datasource, keymapTable);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		var seq = datasource.Destination.Sequence;
		sq.Select(() =>
		{
			var c = new CaseExpression();
			c.When(new ColumnValue(d, HoldJudgementColumnName).False()).Then(new LiteralValue(seq.Command));
			return c;
		}).As(seq.Column);
		sq.Select(d);

		//Re-held data is not subject to extraction.
		sq.Where(() => new ColumnValue(d, HoldJudgementColumnName).False());

		if (injector != null) sq = injector(sq);

		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);

		Logger?.LogInformation("create table sql : {Sql}", cq.ToCommand().CommandText);

		Connection.Execute(cq);

		return GetSelectQuery(bridgeName, columns);
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

		var cnt = Connection.ExecuteScalar<int>(q);
		Logger?.LogInformation("count : {Count} row(s)", cnt);
		return cnt;
	}

	/// <summary>
	/// SELECT columns FROM datasource WHERE not_forwarded
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="keymapTable"></param>
	/// <returns></returns>
	private SelectQuery GetFilteredDatasourceQuery(IDatasource ds, string keymapTable)
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, fullds) = new SelectQuery(ds.Query).ToCTE("_full_datasource");
		var (f, d) = sq.From(fullds).As("d");
		sq.Select(d);

		//If there is no column for hold judgment, it is fixed to "false".
		//In other words, all are treated as transfer targets.
		if (!sq.SelectClause!.Where(x => x.Alias.IsEqualNoCase(HoldJudgementColumnName)).Any())
		{
			sq.Select("false").As(HoldJudgementColumnName);
		}

		//ignore_column

		sq.Where(() => GetHoldCondition(ds, d));
		return sq;
	}

	/// <summary>
	/// SELECT columns FROM bridgeTable
	/// </summary>
	/// <param name="bridgeTable"></param>
	/// <param name="columns"></param>
	/// <returns></returns>
	private static SelectQuery GetSelectQuery(string bridgeTable, List<string> columns)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeTable).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}

	/// <summary>
	/// (datasourceTable.key1, datasourceTable.key2) IN (SELECT hld.key1, hld.key2 FROM holdTable AS hld)
	/// </summary>
	/// <param name="holdTable"></param>
	/// <param name="datasourceSeqColumn"></param>
	/// <returns></returns>
	private static ValueBase GetHoldCondition(IDatasource ds, SelectableTable datasourceTable)
	{
		var sq = new SelectQuery();
		var (_, hld) = sq.From(ds.HoldTable.GetTableFullName()).As("hld");
		ds.KeyColumns.ForEach(key => sq.Select(hld, key));

		var keys = new ValueCollection();
		ds.KeyColumns.ForEach(key => keys.Add(new ColumnValue(datasourceTable, key)));
		var exp = new InExpression(keys, sq.ToValue());

		return exp;
	}
}
