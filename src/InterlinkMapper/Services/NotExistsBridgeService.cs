using Carbunql;
using Carbunql.Building;
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

	public string HoldJudgmentColumnName { get; init; }

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

	public int GetCount(SelectQuery bridgeQuery)
	{
		var q = bridgeQuery.ToCountQuery();
		Logger?.LogInformation("count sql : {Sql}", q.ToCommand().CommandText);

		var cnt = Connection.ExecuteScalar<int>(q);
		Logger?.LogInformation("count : {Count} row(s)", cnt);
		return cnt;
	}

	private SelectQuery GetFilteredDatasourceQuery(IDatasource ds, string keymapTable)
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(ds.Query).ToCTE("_full_datasource");
		var (f, d) = sq.From(cteDatasource).As("d");
		sq.Select(d);

		if (!sq.SelectClause!.Where(x => x.Alias.IsEqualNoCase(HoldJudgmentColumnName)).Any())
		{
			sq.Select("false").As(HoldJudgmentColumnName);
		}

		if (string.IsNullOrEmpty(keymapTable)) return sq;

		if (ds.IsSequence && ds.KeyColumns.Count == 1)
		{
			var seq = ds.KeyColumns.First();

			//WHERE (SELECT MAX(m.seq) FROM map AS m) < d.key
			sq.Where(() =>
			{
				var subq = new SelectQuery();
				subq.From(keymapTable).As("m");
				subq.Select($"max(m.{seq})");

				return subq.ToValue().AddOperatableValue("<", new ColumnValue(d, "key"));
			});
			return sq;
		};

		if (ds.KeyColumns.Any())
		{
			var key = ds.KeyColumns[0];

			//LEFT JOIN map AS m ON d.key1 = m.key1 AND d.key2 = m.key2
			//WHERE m.key1 IS NULL
			var m = f.LeftJoin(keymapTable).As("m").On(d, ds.KeyColumns);
			sq.Where(m, key).IsNull();

			return sq;
		};

		//no filter
		return sq;
	}

	private static SelectQuery GetSelectQuery(string bridgeName, List<string> columns)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeName).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}
}
