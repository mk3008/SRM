using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Extensions;
using Carbunql.Values;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class ReverseTransferService
{
	public ReverseTransferService(IDbConnection cn, int tranId, Datasource ds, string bridgeName, string procMapTableName, string procTableName, string procIdColumnName, string tranIdColumnName, string placeHolderIdentifer, Func<SelectQuery, SelectQuery>? injector = null)
	{
		Connection = cn;
		DS = ds;
		BridgeName = bridgeName;
		TransactionId = tranId;
		ProcessMapTableName = procMapTableName;
		ProcessTableName = procTableName;
		ProcessIdColumnName = procIdColumnName;
		TransactionIdColumnName = tranIdColumnName;
		PlaceholderIdentifer = placeHolderIdentifer;

		if (injector != null) Injector = injector;
	}

	public ReverseTransferService(IDbConnection cn, int tranId, Datasource ds, string bridgeName, Database db, Func<SelectQuery, SelectQuery>? injector = null)
	{
		Connection = cn;
		DS = ds;
		BridgeName = bridgeName;
		TransactionId = tranId;
		ProcessMapTableName = db.ProcessMapNameBuilder(ds.Destination);
		ProcessTableName = db.ProcessTableName;
		ProcessIdColumnName = db.ProcessIdColumnName;
		TransactionIdColumnName = db.TransctionIdColumnName;
		PlaceholderIdentifer = db.PlaceholderIdentifier;

		if (injector != null) Injector = injector;
	}

	private IDbConnection Connection { get; init; }

	public int TransactionId { get; init; }

	public string BridgeName { get; init; }

	public Datasource DS { get; init; }

	private string DatasourceQuery => DS.Query;

	private List<string> KeyColumns => DS.KeyColumns;

	private Destination Destination => DS.Destination;

	private string DestinationTableName => Destination.Table.TableFullName;

	//private bool IsSequenceDatasource => Datasource.IsSequenceDatasource;

	private string? KeyMapTableName => DS.KeyMapTable?.TableFullName;

	private Sequence Sequence => Destination.Sequence;

	private Func<SelectQuery, SelectQuery>? Injector { get; set; }

	private string ProcessMapTableName { get; init; }

	private string ProcessTableName { get; init; }

	private string ProcessIdColumnName { get; init; }

	private string TransactionIdColumnName { get; init; }

	private string PlaceholderIdentifer { get; init; }

	private ReverseOption ReverseOption => Destination.ReverseOption;

	public bool IsStarted { get; private set; } = false;

	public SelectQuery Query => GetSelectQuery();

	private List<string> Columns { get; set; } = new();

	public SelectQuery Initialize()
	{
		if (IsStarted) throw new InvalidOperationException();

		var q = GetFilteredDatasourceQuery();

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		sq.Select(d);

		if (Injector != null) sq = Injector(sq);

		sq.SelectClause!.ToList().ForEach(x => Columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(BridgeName, isTemporary: true);

		Connection.Execute(cq);

		IsStarted = true;

		return GetSelectQuery();
	}

	public void ReverseTransfer()
	{

	}

	public void AllowRetarnsfer()
	{

	}

	public void MapWithDatasource()
	{

	}

	public void MapWithProcess()
	{

	}



	private SelectQuery GetSelectQuery()
	{
		if (!IsStarted) throw new InvalidOperationException();

		var sq = new SelectQuery();
		var (_, b) = sq.From(BridgeName).As("b");

		Columns.ForEach(x => sq.Select(b, x));

		return sq;
	}

	private SelectQuery GetFilteredDatasourceQuery()
	{
		var tmpq = GetDifferenceDetailsDatasourceQuery();
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

	private SelectQuery GetDifferenceDetailsDatasourceQuery()
	{
		var previousq = GetPreviousDatasourceQuery();

		//WITH
		//_previous as (select previous data)
		//_current as (select current data)
		var sq = new SelectQuery();
		var ctePrevious = sq.With(previousq).As("_previous");
		var cteCurrent = sq.With(new SelectQuery(DatasourceQuery)).As("_current");

		//FROM _previous AS p
		//LEFT JOIN _current AS c ON p.key = c.key
		var (from, p) = sq.From(ctePrevious).As("p");
		var c = from.LeftJoin(cteCurrent).As("c").On(p, KeyColumns);

		//SELECT reversable values
		ReverseOption.ReversalColumns.ForEach(x => sq.Select(() =>
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
			exp.When(new ColumnValue(c, KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
			exp.Else(new LiteralValue("false"));
			return exp;
		}).As("_deleted");

		//SELECT value flag
		var commonColumns = Destination!.GetDifferenceCheckColumns().Where((x) => x.IsEqualNoCase(cteCurrent.GetColumnNames())).ToList();
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

	private SelectQuery GetPreviousDatasourceQuery()
	{
		if (string.IsNullOrEmpty(KeyMapTableName)) throw new Exception();

		//FROM destination AS d
		//INNER JOIN keymap AS l, ON d.destination_id = pm.destination_id
		//INNER JOIN porcessmap AS pm ON d.destination_id = pm.destination_id
		//INNER JOIN process AS p ON pm.process_id = p.process_id
		var sq = new SelectQuery();
		var (from, d) = sq.From(DestinationTableName).As("d");
		var km = from.InnerJoin(KeyMapTableName).As("km").On(d, Sequence.Column);
		var pm = from.InnerJoin(ProcessMapTableName).As("pm").On(d, Sequence.Column);
		var p = from.InnerJoin(ProcessTableName).As("p").On(pm, ProcessIdColumnName);

		//SELECT previous datasource values
		sq.Select(d);

		//SELECT datasource keys
		KeyColumns.ForEach(x => sq.Select(km, x));

		//WHERE p.transaction_id = :transaction_id
		var pname = PlaceholderIdentifer + TransactionIdColumnName;
		sq.Where(p, TransactionIdColumnName).Equal(pname);
		sq.Parameters.Add(pname, TransactionId);

		return sq;
	}
}
