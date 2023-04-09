using Carbunql;
using Carbunql.Building;
using Carbunql.Clauses;
using Carbunql.Dapper;
using Carbunql.Extensions;
using Carbunql.Values;
using InterlinkMapper.Data;
using InterlinkMapper.TableAndMap;
using System.Data;
using System.Diagnostics;

namespace InterlinkMapper;

public class Database
{
	public string BatchTransctionTableName { get; set; } = "im_transactions";

	public string BatchProcessTableName { get; set; } = "im_processes";


	public string BatchTransctionIdColumnName { get; set; } = "transaction_id";

	public string BatchProcessIdColumnName { get; set; } = "process_id";

	public string DestinationIdColumnName { get; set; } = "destination_id";

	public string DatasourceIdColumnName { get; set; } = "datasource_id";

	public string ArgumentsColumnName { get; set; } = "arguments";

	public string PlaceholderIdentifier { get; set; } = ":";

	public Func<Destination, string> ProcessMapNameBuilder { get; set; } = (dest) => dest.Table!.TableFullName + "__proc";

	public Func<Datasource, string> KeyMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__key_" + ds.DatasourceName;

	public Func<Datasource, string> HoldMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__hold_" + ds.DatasourceName;

	public Func<Datasource, string> RelationMapNameBuilder { get; set; } = (ds) => ds.Destination!.Table!.TableFullName + "__rel_" + ds.DatasourceName;

	public CreateTableQuery BuildCreateTableQueryAsDifference(int transactionId, string bridgeName, Datasource datasource, Func<SelectQuery, SelectQuery>? injector = null)
	{
		if (datasource.Destination == null) throw new NullReferenceException(nameof(Destination));
		var dest = datasource.Destination;

		if (dest.Table == null) throw new NullReferenceException(nameof(dest.Table));
		if (dest.Sequence == null) throw new NullReferenceException(nameof(dest.Sequence));

		var q = BuildSelectDatasourceQueryAsDifference(transactionId, datasource);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(d);

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(bridgeName, isTemporary: true);
	}

	private SelectQuery BuildSelectDatasourceQueryAsDifference(int transactionId, Datasource datasource)
	{
		if (datasource == null) throw new ArgumentNullException();
		if (datasource.Destination == null) throw new ArgumentNullException();
		var dest = datasource.Destination;

		var previousq = BuildSelectPreviousDatasourceQuery(transactionId, datasource);

		//WITH
		//_previous as (select previous data)
		//_current as (select current data)
		var sq = new SelectQuery();
		var ctePrevious = sq.With(previousq).As("_previous");
		var cteCurrent = sq.With(new SelectQuery(datasource.Query)).As("_current");

		//FROM _previous AS p
		//LEFT JOIN _current AS c ON p.key = c.key
		var (from, p) = sq.From(ctePrevious).As("p");
		var c = from.LeftJoin(cteCurrent).As("c").On(p, datasource.KeyColumns);

		sq.Select(() =>
		{
			var exp = new CaseExpression();
			exp.When(new ColumnValue(c, datasource.KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
			exp.Else(new LiteralValue("false"));
			return exp;
		}).As("_is_deleted");

		dest.ReverseOption.ReversalColumns.ForEach(x => sq.Select(() =>
		{
			var v = new ColumnValue(p, x);
			v.AddOperatableValue("*", new LiteralValue("-1"));
			return v;
		}).As(x));

		sq.Select(p);

		//create condition
		//removed changed
		var ce = new CaseExpression();
		ce.When(new ColumnValue(c, datasource.KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
		ce.Else(new LiteralValue("false"));
		ValueBase condition = ce;

		//value changed
		var commonColumns = dest!.GetDifferenceCheckColumns().Where((string x) => x.IsEqualNoCase(cteCurrent.GetColumnNames())).ToList();
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

	private SelectQuery BuildSelectPreviousDatasourceQuery(int transactionId, Datasource datasource)
	{
		if (datasource == null) throw new ArgumentNullException();
		if (datasource.Destination == null) throw new ArgumentNullException();
		var dest = datasource.Destination;

		var keyTable = KeyMapNameBuilder(datasource);
		var procMapTable = ProcessMapNameBuilder(dest);
		var procTable = BatchProcessTableName;
		var procIdColumnName = BatchProcessIdColumnName;
		var tranIdColumnName = BatchTransctionIdColumnName;

		//FROM destination AS d
		//INNER JOIN key_map AS l, ON d.destination_id = pm.destination_id
		//INNER JOIN porcess_map AS pm ON d.destination_id = pm.destination_id
		//INNER JOIN process AS p ON pm.process_id = p.process_id
		var sq = new SelectQuery();
		var (from, d) = sq.From(dest!.Table.TableFullName).As("d");
		var keymap = from.InnerJoin(keyTable).As("km").On(d, dest.Sequence.Column);
		var procmap = from.InnerJoin(procMapTable).As("pm").On(d, dest.Sequence.Column);
		var proc = from.InnerJoin(procTable).As("p").On(procmap, procIdColumnName);

		//SELECT d.*, km.key
		sq.Select(d);
		datasource.KeyColumns.ForEach(x => sq.Select(keymap, x));

		//WHERE p.transaction_id = :transaction_id
		var pname = PlaceholderIdentifier + tranIdColumnName;
		sq.Where(proc, tranIdColumnName).Equal(pname);
		sq.Parameters.Add(pname, transactionId);

		return sq;
	}
}
