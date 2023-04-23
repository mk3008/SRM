using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
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
	public NotExistsBridgeService(IDbConnection cn, ILogger? logger = null)
	{
		Connection = cn;
		Logger = logger;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	public string GenerateBridgeName(Datasource datasource)
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

	public SelectQuery CreateAsNew(Datasource datasource, string bridgeName, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var keymapTable = datasource.KeyMapTable.TableFullName;

		var q = GetFilteredDatasourceQuery(datasource, keymapTable);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		var seq = datasource.Destination.Sequence;
		sq.Select(seq.Command).As(seq.Column);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);

		Logger?.LogInformation(cq.ToCommand().CommandText);

		Connection.Execute(cq);

		return GetSelectQuery(bridgeName, columns);
	}

	public int GetCount(SelectQuery bridgeQuery)
	{
		var q = bridgeQuery.ToCountQuery();
		Logger?.LogInformation(q.ToCommand().CommandText);

		var cnt = Connection.ExecuteScalar<int>(q);
		Logger?.LogInformation($"count : {cnt} row(s)");
		return cnt;
	}

	private SelectQuery GetFilteredDatasourceQuery(Datasource ds, string keymapTable)
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(ds.Query).ToCTE("_full_datasource");
		var (f, d) = sq.From(cteDatasource).As("d");
		sq.Select(d);

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
			//WHERE m.key IS NULL
			var m = f.LeftJoin(keymapTable).As("m").On(d, ds.KeyColumns);
			sq.Where(m, key).IsNull();

			return sq;
		};

		//no filter
		return sq;
	}

	private SelectQuery GetSelectQuery(string bridgeName, List<string> columns)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeName).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}
}
