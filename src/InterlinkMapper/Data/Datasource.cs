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

	private SelectQuery GenerateSelectDatasourceQueryIfNotExists()
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

	private SelectQuery GenerateSelectPreviousData(int transactionId, string procMapTable, string procTable, string keyTable, string procIdColumnName, string tranIdColumnName, string placeholderIndentifer)
	{
		var dest = Destination;

		//FROM destination AS d
		//INNER JOIN key_map AS l, ON d.destination_id = pm.destination_id
		//INNER JOIN porcess_map AS pm ON d.destination_id = pm.destination_id
		//INNER JOIN process AS p ON pm.process_id = p.process_id
		var sq = new SelectQuery();
		var (from, d) = sq.From(Destination!.Table.TableFullName).As("d");
		var keymap = from.InnerJoin(keyTable).As("km").On(d, Destination.Sequence.Column);
		var procmap = from.InnerJoin(procMapTable).As("pm").On(d, Destination.Sequence.Column);
		var proc = from.InnerJoin(procTable).As("p").On(procmap, procIdColumnName);

		//SELECT d.*, km.key
		sq.Select(d);
		KeyColumns.ForEach(x => sq.Select(keymap, x));

		//WHERE p.transaction_id = :transaction_id
		var pname = placeholderIndentifer + tranIdColumnName;
		sq.Where(proc, tranIdColumnName).Equal(pname);
		sq.Parameters.Add(pname, transactionId);

		return sq;
	}

	public SelectQuery GenerateSelectDatasourceQueryIfDifference(int transactionId, string procMapTable, string procTable, string keyTable, string procIdColumnName, string tranIdColumnName, string placeholderIndentifer)
	{
		if (Destination == null) throw new Exception();

		var previousq = GenerateSelectPreviousData(transactionId, procMapTable, procTable, keyTable, procIdColumnName, tranIdColumnName, placeholderIndentifer);

		//WITH
		//_previous as (select previous data)
		//_current as (select current data)
		var sq = new SelectQuery();
		var ctePrevious = sq.With(previousq).As("_previous");
		var cteCurrent = sq.With(new SelectQuery(Query)).As("_current");

		//FROM _previous AS p
		//LEFT JOIN _current AS c ON p.key = c.key
		var (from, p) = sq.From(ctePrevious).As("p");
		var c = from.LeftJoin(cteCurrent).As("c").On(p, KeyColumns);

		sq.Select(() =>
		{
			var exp = new CaseExpression();
			exp.When(new ColumnValue(c, KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
			exp.Else(new LiteralValue("false"));
			return exp;
		}).As("_is_deleted");
		Destination.ReverseOption.ReversalColumns.ForEach(x => sq.Select(() =>
		{
			var v = new ColumnValue(p, x);
			v.AddOperatableValue("*", new LiteralValue("-1"));
			return v;
		}).As(x));
		sq.Select(p);

		//create condition
		//removed changed
		var ce = new CaseExpression();
		ce.When(new ColumnValue(c, KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
		ce.Else(new LiteralValue("false"));
		ValueBase condition = ce;

		//value changed
		var commonColumns = Destination!.GetDifferenceCheckColumns().Where((string x) => x.IsEqualNoCase(cteCurrent.GetColumnNames())).ToList();
		commonColumns.ForEach(x =>
		{
			//CASE WHEN is_changed THEN true ElSE false END
			var prevValue = new ColumnValue(p, x);
			var currentValue = new ColumnValue(c, x);

			var exp = new CaseExpression();
			exp.When(prevValue.IsNull().And(currentValue.IsNull())).Then(new LiteralValue("false"));
			exp.When(prevValue.IsNull().And(currentValue.IsNotNull())).Then(new LiteralValue("true"));
			exp.When(prevValue.IsNotNull().And(currentValue.IsNull())).Then(new LiteralValue("true"));
			exp.When(prevValue.Equal(currentValue)).Then(new LiteralValue("false"));
			exp.When(prevValue.NotEqual(currentValue)).Then(new LiteralValue("false"));

			condition = condition.Or(exp);
		});
		// WHERE (CASE WHEN .. END OR CASE WHEN .. END OR CASE WHEN .. END)
		if (condition != null) sq.Where(condition.ToGroup());

		return sq;
	}

	public CreateTableQuery GenerateCreateBridgeQueryAsDifferent(int transactionId, string procMapTable, string procTable, string keyTable, string procIdColumnName, string tranIdColumnName, string placeholderIndentifer, string bridgeName, Func<SelectQuery, SelectQuery>? injector)
	{
		if (Destination == null) throw new NullReferenceException(nameof(Destination));
		if (Destination.Table == null) throw new NullReferenceException(nameof(Destination.Table));
		if (Destination.Sequence == null) throw new NullReferenceException(nameof(Destination.Sequence));

		var q = GenerateSelectDatasourceQueryIfDifference(transactionId, procMapTable, procTable, keyTable, procIdColumnName, tranIdColumnName, placeholderIndentifer);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(d);

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(bridgeName, isTemporary: true);
	}

	public CreateTableQuery GenerateCreateBridgeQuery(string bridgeName, Func<SelectQuery, SelectQuery>? injector)
	{
		if (Destination == null) throw new NullReferenceException(nameof(Destination));
		if (Destination.Table == null) throw new NullReferenceException(nameof(Destination.Table));
		if (Destination.Sequence == null) throw new NullReferenceException(nameof(Destination.Sequence));

		var q = GenerateSelectDatasourceQueryIfNotExists();

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(Destination.Sequence.Command).As(Destination.Sequence.Column);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(bridgeName, isTemporary: true);
	}

	public SelectQuery GenerateSelectBridgeQuery(string bridgeName)
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
}