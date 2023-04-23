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

public class NotExistsBridgeService
{
	public NotExistsBridgeService(IDbConnection cn, ILogger? logger = null, string holdJudgmentColumnName = "")
	{
		Connection = cn;
		Logger = logger;
		HoldJudgmentColumnName = !string.IsNullOrEmpty(holdJudgmentColumnName) ? holdJudgmentColumnName : "_is_hold";
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	private string HoldJudgmentColumnName { get; init; }

	/// <summary>
	/// Generate a bridge name.
	/// </summary>
	/// <param name="datasource"></param>
	/// <returns></returns>
	public static string GenerateBridgeName(IDatasource datasource)
	{
		using MD5 md5Hash = MD5.Create();

		byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(datasource.DatasourceName));
		var sb = ZString.CreateStringBuilder();
		sb.Append("_");
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
	/// <param name="bridgeName"></param>
	/// <param name="injector"></param>
	/// <returns></returns>
	public SelectQuery CreateAsNew(IDatasource datasource, string bridgeName, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var keymapTable = datasource.KeyMapTable.TableFullName;

		var q = GetFilteredDatasourceQuery(datasource, keymapTable);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		var seq = datasource.Destination.Sequence;
		sq.Select(() =>
		{
			var c = new CaseExpression();
			c.When(new ColumnValue(d, HoldJudgmentColumnName).False()).Then(new LiteralValue(seq.Command));
			return c;
		}).As(seq.Column);
		sq.Select(d);

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
		if (!sq.SelectClause!.Where(x => x.Alias.IsEqualNoCase(HoldJudgmentColumnName)).Any())
		{
			sq.Select("false").As(HoldJudgmentColumnName);
		}

		//If there is no keymap, select all (assuming proper filtering in the datasource query).
		if (string.IsNullOrEmpty(keymapTable)) return sq;

		//If Sequence transfer is supported, simplify forwarded key determination.
		if (ds.IsSupportSequenceTransfer && ds.KeyColumns.Count == 1)
		{
			var seq = ds.KeyColumns.First();

			//WHERE (SELECT COALESCE(MAX(m.seq), 0) AS seq FROM map AS m) < d.key
			sq.Where(() =>
			{
				var v = GetMaxIdOrDefaultValue(keymapTable, seq);
				v.AddOperatableValue("<", new ColumnValue(d, seq));
				return v;
			});
			return sq;
		};

		//If Sequence transfer is not supported, check individual keys.
		if (ds.KeyColumns.Any())
		{
			var key = ds.KeyColumns.First();

			//LEFT JOIN map AS m ON d.key1 = m.key1 AND d.key2 = m.key2
			//WHERE m.key1 IS NULL
			var m = f.LeftJoin(keymapTable).As("m").On(d, ds.KeyColumns);
			sq.Where(m, key).IsNull();

			return sq;
		};

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
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(columns);
		return sq;
	}

	/// <summary>
	/// (SELECT COALESCE(MAX(datasourceSeqColumn), 0) FROM keymapTable)
	/// </summary>
	/// <param name="keymapTable"></param>
	/// <param name="datasourceSeqColumn"></param>
	/// <returns></returns>
	private static ValueBase GetMaxIdOrDefaultValue(string keymapTable, string datasourceSeqColumn)
	{
		var sq = new SelectQuery();
		sq.From(keymapTable).As("m");
		sq.Select($"coalesce(max(m.{datasourceSeqColumn}),0)").As(datasourceSeqColumn);
		return sq.ToValue();
	}
}
