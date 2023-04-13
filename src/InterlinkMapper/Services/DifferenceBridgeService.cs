using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Extensions;
using Carbunql.Values;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class DifferenceBridgeService
{
	public DifferenceBridgeService(IDbConnection cn, Database db)
	{
		Connection = cn;

		PlaceholderIdentifer = db.PlaceholderIdentifier;

		ProcessTable = db.ProcessTableName;
		ProcessIdColumn = db.ProcessIdColumnName;
		TransactionIdColumn = db.TransctionIdColumnName;
	}

	private IDbConnection Connection { get; init; }

	public string PlaceholderIdentifer { get; init; }

	private string ProcessTable { get; init; }

	private string ProcessIdColumn { get; init; }

	private string TransactionIdColumn { get; init; }

	public SelectQuery CreateAsNew(Datasource datasource, string bridgeName, int transactionId, string processmapTable, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var q = GetFilteredDatasourceQuery(datasource, transactionId, processmapTable);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(d);

		if (injector != null) sq = injector(sq);

		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);

		Connection.Execute(cq);

		return GetSelectQuery(bridgeName, columns);
	}

	private SelectQuery GetFilteredDatasourceQuery(Datasource datasource, int transactionId, string processmapTable)
	{
		var tmpq = GetDifferenceDetailsDatasourceQuery(datasource, transactionId, processmapTable);
		var sq = new SelectQuery();

		var (from, d) = sq.From(tmpq).As("d");
		sq.Select(d);

		//WHERE
		//CASE WHEN d._deleted = TRUE THEN TRUE
		//CASE WHEN d._changed_v1 = TRUE THEN TRUE
		//...
		//ELSE FALSE
		//END
		var exp = new CaseExpression();
		tmpq.SelectClause!.Where(x => x.Alias.StartsWith("_")).ToList().ForEach(x =>
		{
			exp.When(new ColumnValue(d, x.Alias).True).Then(new LiteralValue("true"));
		});
		exp.Else(new LiteralValue("false"));
		sq.Where(exp);

		return sq;
	}

	private SelectQuery GetDifferenceDetailsDatasourceQuery(Datasource datasource, int transactionId, string processmapTable)
	{
		var previousq = GetPreviousDatasourceQuery(datasource, transactionId, processmapTable);

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

		//SELECT reversable values
		datasource.Destination.ReverseOption.ReversalColumns.ForEach(x => sq.Select(() =>
		{
			var v = new ColumnValue(p, x);
			v.AddOperatableValue("*", new LiteralValue("-1"));
			return v;
		}).As(x));

		//SELECT other values
		sq.Select(p, overwrite: false);

		//SELECT removed flag
		sq.Select(() =>
		{
			var exp = new CaseExpression();
			exp.When(new ColumnValue(c, datasource.KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
			exp.Else(new LiteralValue("false"));
			return exp;
		}).As("_deleted");

		//SELECT value flag
		var commonColumns = datasource.Destination!.GetDifferenceCheckColumns().Where((x) => x.IsEqualNoCase(cteCurrent.GetColumnNames())).ToList();
		commonColumns.ForEach(x =>
		{
			//CASE WHEN value is changed THEN TRUE ElSE FALSE END AS _changed_val
			var prevValue = new ColumnValue(p, x);
			var currentValue = new ColumnValue(c, x);

			var exp = new CaseExpression();
			exp.When(prevValue.IsNull().And(currentValue.IsNull())).Then(new LiteralValue("false"));
			exp.When(prevValue.IsNull().And(currentValue.IsNotNull())).Then(new LiteralValue("true"));
			exp.When(prevValue.IsNotNull().And(currentValue.IsNull())).Then(new LiteralValue("true"));
			exp.When(prevValue.Equal(currentValue)).Then(new LiteralValue("false"));
			exp.When(prevValue.NotEqual(currentValue)).Then(new LiteralValue("false"));

			sq.Select(exp).As("_changed_" + x);
		});

		return sq;
	}

	private SelectQuery GetPreviousDatasourceQuery(Datasource ds, int transactionId, string processmapTable)
	{
		if (string.IsNullOrEmpty(ds.KeyMapTable.TableFullName)) throw new Exception();

		var seq = ds.Destination.Sequence;
		//FROM destination AS d
		//INNER JOIN keymap AS l, ON d.destination_id = pm.destination_id
		//INNER JOIN porcessmap AS pm ON d.destination_id = pm.destination_id
		//INNER JOIN process AS p ON pm.process_id = p.process_id
		var sq = new SelectQuery();
		var (from, d) = sq.From(ds.Destination.Table.TableFullName).As("d");
		var km = from.InnerJoin(ds.KeyMapTable.TableFullName).As("km").On(d, seq.Column);
		var pm = from.InnerJoin(processmapTable).As("pm").On(d, seq.Column);
		var p = from.InnerJoin(ProcessTable).As("p").On(pm, ProcessIdColumn);

		//SELECT previous datasource values
		sq.Select(d);

		//SELECT datasource keys
		ds.KeyColumns.ForEach(x => sq.Select(km, x));

		//WHERE p.transaction_id = :transaction_id
		var pname = PlaceholderIdentifer + TransactionIdColumn;
		sq.Where(p, TransactionIdColumn).Equal(pname);
		sq.Parameters.Add(pname, transactionId);

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
