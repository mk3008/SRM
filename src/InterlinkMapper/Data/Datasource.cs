using Carbunql;
using Carbunql.Building;
using Carbunql.Clauses;
using Carbunql.Extensions;
using Carbunql.Values;

namespace InterlinkMapper.Data;

public class Datasource
{
	public int DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public Destination? Destination { get; set; }

	public string Query { get; set; } = string.Empty;

	public DbTable? KeyMapTable { get; set; }

	public DbTable? RelationTable { get; set; }

	public bool IsSequenceDatasource { get; set; } = false;

	public List<string> KeyColumns { get; set; } = new();

	public SelectQuery BuildSelectBridgeQuery(string bridgeName)
	{
		if (Destination == null) throw new NullReferenceException(nameof(Destination));
		if (Destination.Sequence == null) throw new NullReferenceException(nameof(Destination.Sequence));

		var tmp = new SelectQuery(Query);
		var cols = tmp.SelectClause!.Select(x => x.Alias).ToList();

		var sq = new SelectQuery();
		var (f, b) = sq.From(bridgeName).As("b");

		sq.Select(b);

		return sq;
	}

	public CreateTableQuery BuildCreateBridgeTableQuery(string bridgeName, Func<SelectQuery, SelectQuery>? injector)
	{
		if (Destination == null) throw new NullReferenceException(nameof(Destination));
		if (Destination.Table == null) throw new NullReferenceException(nameof(Destination.Table));
		if (Destination.Sequence == null) throw new NullReferenceException(nameof(Destination.Sequence));

		var q = BuildSelectDatasourceQueryIfNotExists();

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(Destination.Sequence.Command).As(Destination.Sequence.Column);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(bridgeName, isTemporary: true);
	}

	private SelectQuery BuildSelectDatasourceQueryIfNotExists()
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(Query).ToCTE("_full_datasource");
		var (f, d) = sq.From(cteDatasource).As("d");
		sq.SelectAll(d);

		if (KeyMapTable == null) return sq;

		if (IsSequenceDatasource && KeyColumns.Count == 1)
		{
			var seq = KeyColumns[0];

			//WHERE (SELECT MAX(m.seq) FROM map AS m) < d.key
			sq.Where(() =>
			{
				var subq = new SelectQuery();
				subq.From(KeyMapTable.TableFullName).As("m");
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
			var m = f.LeftJoin(KeyMapTable.TableFullName).As("m").On(d, KeyColumns);
			sq.Where(m, key).IsNull();

			return sq;
		};

		//no filter
		return sq;
	}
}