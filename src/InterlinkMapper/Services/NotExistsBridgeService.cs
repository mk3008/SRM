﻿using Carbunql;
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
	public NotExistsBridgeService(IDbConnection cn, ILogger? logger = null, string holdJudgmentColumnName = "_hold")
	{
		Connection = cn;
		Logger = logger;
		HoldJudgmentColumnName = holdJudgmentColumnName;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	private string HoldJudgmentColumnName { get; init; }

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
		sb.Append("_nx_");
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
	public SelectQuery CreateAsNew(IDatasource datasource, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var bridgeName = GenerateBridgeName(datasource);

		var sq = new SelectQuery();
		var (_, d) = sq.From(GetSelectDatasourceQueryForTransfer(datasource)).As("d");

		sq.Select(d);

		//assign a Sequence to the transfer target
		var seq = datasource.Destination.Sequence;
		if (sq.SelectClause!.Where(x => x.Alias.IsEqualNoCase(HoldJudgmentColumnName)).Any())
		{
			sq.Select(seq.Command).As(seq.Column);
		}
		else
		{
			sq.Select(() =>
			{
				var c = new CaseExpression();
				c.When(new ColumnValue(d, HoldJudgmentColumnName).False()).Then(new LiteralValue(seq.Command));
				return c;
			}).As(seq.Column);
		}

		if (injector != null) sq = injector(sq);

		//remember as a column created in the bridge table
		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);
		Logger?.LogInformation("create table sql : {Sql}", cq.ToCommand().CommandText);
		Connection.Execute(cq);

		return GetSelectBridgeQuery(bridgeName, columns);
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
	private SelectQuery GetSelectDatasourceQueryForTransfer(IDatasource ds)
	{
		var keymapTable = ds.KeyMapTable.GetTableFullName();
		var sq = GetSelectDatasourceQuery(ds);

		//If there is no keymap, select all (assuming proper filtering in the datasource query).
		if (string.IsNullOrEmpty(keymapTable) || !ds.KeyColumns.Any()) return sq;

		if (ds.IsSupportSequenceTransfer && ds.KeyColumns.Count == 1)
		{
			//If Sequence transfer is supported, simplify forwarded key determination.
			AddConditionAsGreaterThanTransferedSequence(ds, sq);
		}
		else
		{
			//If Sequence transfer is not supported, check individual keys.
			AddConditionAsNotExistInKeymap(ds, sq);
		}
		return sq;
	}

	private static SelectQuery GetSelectDatasourceQuery(IDatasource ds)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(new SelectQuery(ds.Query)).As("d");
		sq.Select(d);
		return sq;
	}

	private static SelectQuery GetSelectBridgeQuery(string bridgeTable, List<string> columns)
	{
		//WHERE (SELECT COALESCE(MAX(datasourceSeqColumn), 0) FROM keymapTable) < datasourceTable.datasourceSeqColumn
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeTable).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}

	private static void AddConditionAsGreaterThanTransferedSequence(IDatasource ds, SelectQuery query)
	{
		//WHERE (SELECT COALESCE(MAX(datasourceSeqColumn), 0) FROM keymapTable) < datasourceTable.datasourceSeqColumn
		var from = query.FromClause;
		if (from == null) throw new NullReferenceException(nameof(from));
		var dsTable = from.Root;

		var seqColumn = ds.KeyColumns.First();

		var sq = new SelectQuery();
		sq.From(ds.KeyMapTable.GetTableFullName()).As("m");
		sq.Select($"coalesce(max(m.{seqColumn}),0)").As(seqColumn);
		var v = sq.ToValue();
		v.AddOperatableValue("<", new ColumnValue(dsTable, seqColumn));

		query.Where(v);
	}

	private static void AddConditionAsNotExistInKeymap(IDatasource ds, SelectQuery query)
	{
		// LEFT JOIN map AS m ON d.key1 = m.key1 AND d.key2 = m.key2
		// WHERE m.key1 IS NULL
		var from = query.FromClause;
		if (from == null) throw new NullReferenceException(nameof(from));
		var dsTable = from.Root;

		var keymapTable = ds.KeyMapTable.GetTableFullName();
		var key = ds.KeyColumns.First();

		var m = from.LeftJoin(keymapTable).As("m").On(dsTable, ds.KeyColumns);
		query.Where(m, key).IsNull();
	}
}
