using Carbunql;
using Carbunql.Building;
using Carbunql.Values;
using InterlinkMapper.Data;

namespace InterlinkMapper;

public class NotExistsBridge
{
	public NotExistsBridge(Datasource datasource, string bridgeName, Func<SelectQuery, SelectQuery>? injector = null)
	{
		Datasource = datasource;
		BridgeName = bridgeName;
		if (injector != null) Injector = injector;
	}

	public string BridgeName { get; init; }

	public Datasource Datasource { get; init; }

	private string Query => Datasource.Query;

	private List<string> KeyColumns => Datasource.KeyColumns;

	private Destination Destination => Datasource.Destination;

	private bool IsSequenceDatasource => Datasource.IsSequenceDatasource;

	private string? KeyMapTableName => Datasource.KeyMapTable?.TableFullName;

	private Sequence Sequence => Destination.Sequence;

	private Func<SelectQuery, SelectQuery>? Injector { get; set; }

	public SelectQuery GetSelectQuery()
	{
		var q = new SelectQuery(Query);
		var cols = q.SelectClause!.Select(x => x.Alias).ToList();

		var sq = new SelectQuery();
		var (_, b) = sq.From(BridgeName).As("b");

		sq.Select(b, Sequence.Column);
		cols.ForEach(x => sq.Select(b, x));

		return sq;
	}

	public CreateTableQuery GetCreateTableQuery()
	{
		var q = GetDatasourceQuery();

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(Sequence.Command).As(Sequence.Column);
		sq.Select(d);

		if (Injector != null) sq = Injector(sq);

		return sq.ToCreateTableQuery(BridgeName, isTemporary: true);
	}

	private SelectQuery GetDatasourceQuery()
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(Query).ToCTE("_full_datasource");
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
