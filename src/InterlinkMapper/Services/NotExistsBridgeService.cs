using Carbunql;
using Carbunql.Building;
using Carbunql.Extensions;
using Carbunql.Values;
using Cysharp.Text;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper.Services;

public class NotExistsBridgeService : IQueryExecuteService
{
	public NotExistsBridgeService(SystemEnvironment environment, IDbConnection cn, int processId, ILogger? logger = null)
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

	public int CommandTimeout { get; set; } = 60 * 15;

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
	/// <param name="ds"></param>
	/// <param name="bridgeName"></param>
	/// <param name="injector"></param>
	/// <returns></returns>
	public (SelectQuery SelectBridgeQuery, int Rows) CreateAndSelectBridge(IDatasource ds, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var bridgeName = GenerateBridgeName(ds);

		var sq = new SelectQuery();
		var (_, d) = sq.From(GetSelectDatasourceQueryForTransfer(ds)).As("d");

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

		if (injector != null) sq = injector(sq);

		//remember as a column created in the bridge table
		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cnt = this.CreateTable(sq, bridgeName, isTemporary: true, commandTimeout: CommandTimeout);
		var query = GetSelectBridgeQuery(bridgeName, columns);

		return (query, cnt);
	}

	/// <summary>
	/// SELECT columns FROM datasource WHERE not_forwarded
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="keymapTable"></param>
	/// <returns></returns>
	private SelectQuery GetSelectDatasourceQueryForTransfer(IDatasource ds)
	{
		var sq = GetSelectDatasourceQuery(ds);

		//If there is no keymap, select all (assuming proper filtering in the datasource query).
		var keymapTable = ds.KeyMapTable.GetTableFullName();
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
		if (string.IsNullOrEmpty(keymapTable)) throw new NullReferenceException(nameof(keymapTable));

		var key = ds.KeyColumns.First();

		var m = from.LeftJoin(keymapTable).As("m").On(dsTable, ds.KeyColumns);
		query.Where(m, key).IsNull();
	}
}
