using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Values;
using Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class ForwardTransferService
{
	public ForwardTransferService(IDbConnection cn, Datasource datasource, string bridgeName)
	{
		Connection = cn;
		Datasource = datasource;
		BridgeName = bridgeName;
	}

	private IDbConnection Connection { get; init; }

	public string BridgeName { get; init; }

	public Datasource Datasource { get; init; }

	private string DatasourceQuery => Datasource.Query;

	private List<string> KeyColumns => Datasource.KeyColumns;

	private bool IsSequenceDatasource => Datasource.IsSequence;

	private string? KeyMapTableName => Datasource.KeyMapTable?.TableFullName;

	private Sequence Sequence => Datasource.Destination.Sequence;

	//private Func<SelectQuery, SelectQuery>? Injector { get; set; }

	public SelectQuery Query => GetSelectQuery();

	private List<string> Columns { get; set; } = new();

	public bool IsInitialized { get; private set; } = false;

	public SelectQuery Initalize(Func<SelectQuery, SelectQuery>? injector = null)
	{
		if (IsInitialized) throw new InvalidOperationException();

		var q = GetFilteredDatasourceQuery();

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(Sequence.Command).As(Sequence.Column);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		sq.SelectClause!.ToList().ForEach(x => Columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(BridgeName, isTemporary: true);

		Connection.Execute(cq);

		IsInitialized = true;

		return GetSelectQuery();
	}

	public void ForwardTransfer()
	{

	}

	public void PreventRetransfer()
	{

	}

	public void MapWithDatasource()
	{

	}

	public void MapWithProcess()
	{

	}

	public void HoldRequest()
	{

	}

	private SelectQuery GetSelectQuery()
	{
		if (!IsInitialized) throw new InvalidOperationException();

		var sq = new SelectQuery();
		var (_, b) = sq.From(BridgeName).As("b");

		Columns.ForEach(x => sq.Select(b, x));

		return sq;
	}

	private SelectQuery GetFilteredDatasourceQuery()
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(DatasourceQuery).ToCTE("_full_datasource");
		var (f, d) = sq.From(cteDatasource).As("d");
		sq.SelectAll(d);

		if (string.IsNullOrEmpty(KeyMapTableName)) return sq;

		if (IsSequenceDatasource && KeyColumns.Count == 1)
		{
			var seq = KeyColumns.First();

			//WHERE (SELECT MAX(m.seq) FROM map AS m) < d.key
			sq.Where(() =>
			{
				var subq = new SelectQuery();
				subq.From(KeyMapTableName).As("m");
				subq.Select($"max(m.{seq})");

				return subq.ToValue().AddOperatableValue("<", new ColumnValue(d, "key"));
			});
			return sq;
		};

		if (KeyColumns.Any())
		{
			var key = KeyColumns[0];

			//LEFT JOIN map AS m ON d.key1 = m.key1 AND d.key2 = m.key2
			//WHERE m.key IS NULL
			var m = f.LeftJoin(KeyMapTableName).As("m").On(d, KeyColumns);
			sq.Where(m, key).IsNull();

			return sq;
		};

		//no filter
		return sq;
	}
}
