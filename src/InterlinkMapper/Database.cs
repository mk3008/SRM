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

	public BatchTransaction RegistAndGetBatchTransactionAsNew(IDbConnection cn, Datasource datasource, string arguments)
	{
		if (datasource.Destination == null) throw new ArgumentNullException();

		var dic = new Dictionary<string, object>();
		dic[DestinationIdColumnName] = datasource.Destination!.DestinationId;
		dic[DatasourceIdColumnName] = datasource.DatasourceId;
		dic[ArgumentsColumnName] = arguments;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(BatchTransctionTableName);
		iq.Returning(BatchTransctionIdColumnName);

		var id = cn.Execute(iq);

		return new BatchTransaction(id, datasource, arguments);
	}

	public BatchProcess RegistAndGetBatchProcessAsNew(IDbConnection cn, BatchTransaction trn, Datasource datasource)
	{
		if (datasource.Destination == null) throw new ArgumentNullException();

		var dic = new Dictionary<string, object>();
		dic[BatchTransctionIdColumnName] = trn.TransactionId;
		dic[DestinationIdColumnName] = datasource.Destination!.DestinationId;
		dic[DatasourceIdColumnName] = datasource.DatasourceId;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(BatchProcessTableName);
		iq.Returning(BatchProcessIdColumnName);

		var id = cn.Execute(iq);

		return new BatchProcess(id, trn.TransactionId, datasource);
	}

	public void InsertProcessMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select :process_id as process_id, b.dest_id from _bridge as b
		//where b.dest_id is not null
		var pname = PlaceholderIdentifier + BatchProcessIdColumnName;
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);
		sq.Select(pname).As(BatchProcessIdColumnName);
		sq.Parameters.Add(pname, process.ProcessId);

		sq.Where(b, destIdColumn).IsNotNull();

		//insert into process map
		var iq = sq.ToInsertQuery(BatchProcessTableName);

		cn.Execute(iq);
	}

	public void DeleteProcessMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.datasource_id from _bridge as b
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);

		//delete from key map
		var dq = sq.ToDeleteQuery(ProcessMapNameBuilder(process.Datasource.Destination), new[] { destIdColumn });

		cn.Execute(dq);
	}

	public void DeleteKeyMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.datasource_id from _bridge as b
		var keys = process.Datasource.KeyColumns;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		foreach (var dskey in process.Datasource.KeyColumns) sq.Select(b, dskey);

		//delete from key map
		var dq = sq.ToDeleteQuery(KeyMapNameBuilder(process.Datasource), keys);

		cn.Execute(dq);
	}

	public void DeleteRelationMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.datasource_id from _bridge as b
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);

		//delete from key map
		var dq = sq.ToDeleteQuery(RelationMapNameBuilder(process.Datasource), new[] { destIdColumn });

		cn.Execute(dq);
	}

	public void InsertKeyMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.dest_id, b.datasource_id from _bridge as b
		//where b.dest_id is not null
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);
		foreach (var dskey in process.Datasource.KeyColumns) sq.Select(b, dskey);

		sq.Where(b, destIdColumn).IsNotNull();

		//insert into key map
		var iq = sq.ToInsertQuery(KeyMapNameBuilder(process.Datasource));

		cn.Execute(iq);
	}

	public void InsertKeyMapIfNotExists(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.dest_id, b.datasource_id from _bridge as b
		//where b.dest_id is not null
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);
		foreach (var dskey in process.Datasource.KeyColumns) sq.Select(b, dskey);
		sq.Where(b, destIdColumn).IsNotNull();

		//merge into key map
		var mq = sq.ToMergeQuery(HoldMapNameBuilder(process.Datasource), process.Datasource.KeyColumns);
		mq.AddNotMatchedInsert();
		cn.Execute(mq);
	}

	public void InsertRelationMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.dest_id, b.datasource_id from _bridge as b
		//where b.dest_id is not null
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, destIdColumn);
		foreach (var dskey in process.Datasource.KeyColumns) sq.Select(b, dskey);

		sq.Where(b, destIdColumn).IsNotNull();

		//insert into relation map
		var iq = sq.ToInsertQuery(RelationMapNameBuilder(process.Datasource));

		cn.Execute(iq);
	}

	public void InsertHoldMap(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.datasource_id from _bridge as b
		var destIdColumn = process.Datasource.Destination.Sequence.Column;
		var keys = process.Datasource.KeyColumns;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		foreach (var dskey in keys) sq.Select(b, dskey);

		//merge into hold map
		var mq = sq.ToMergeQuery(HoldMapNameBuilder(process.Datasource), keys);
		mq.AddMatchedDelete(() =>
		{
			return new ColumnValue(mq.DatasourceAlias, destIdColumn).IsNotNull();
		});
		mq.AddNotMatchedInsert();
		cn.Execute(mq);
	}

	public void InsertDestination(IDbConnection cn, BatchProcess process, SelectQuery brigeQuery)
	{
		if (process.Datasource.Destination == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Sequence == null) throw new ArgumentNullException();
		if (process.Datasource.Destination.Table == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select b.dest_id, ... from _bridge as b
		//where b.dest_id is not null
		var destIdColumn = process.Datasource.Destination.Sequence.Column;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(process.Datasource.Destination.Table.Columns);

		sq.Where(b, destIdColumn).IsNotNull();

		//insert into destination
		var iq = sq.ToInsertQuery(process.Datasource.Destination.Table.TableFullName);

		cn.Execute(iq);
	}

	//private SelectQuery GenerateSelectPreviousData(int transactionId, Datasource datasource)
	//{
	//	if (datasource == null) throw new ArgumentNullException();
	//	if (datasource.Destination == null) throw new ArgumentNullException();
	//	var dest = datasource.Destination;

	//	var keyTable = KeyMapNameBuilder(datasource);
	//	var procMapTable = ProcessMapNameBuilder(dest);
	//	var procTable = BatchProcessTableName;
	//	var procIdColumnName = BatchProcessIdColumnName;
	//	var tranIdColumnName = BatchTransctionIdColumnName;

	//	//FROM destination AS d
	//	//INNER JOIN key_map AS l, ON d.destination_id = pm.destination_id
	//	//INNER JOIN porcess_map AS pm ON d.destination_id = pm.destination_id
	//	//INNER JOIN process AS p ON pm.process_id = p.process_id
	//	var sq = new SelectQuery();
	//	var (from, d) = sq.From(dest!.Table.TableFullName).As("d");
	//	var keymap = from.InnerJoin(keyTable).As("km").On(d, dest.Sequence.Column);
	//	var procmap = from.InnerJoin(procMapTable).As("pm").On(d, dest.Sequence.Column);
	//	var proc = from.InnerJoin(procTable).As("p").On(procmap, procIdColumnName);

	//	//SELECT d.*, km.key
	//	sq.Select(d);
	//	datasource.KeyColumns.ForEach(x => sq.Select(keymap, x));

	//	//WHERE p.transaction_id = :transaction_id
	//	var pname = PlaceholderIdentifier + tranIdColumnName;
	//	sq.Where(proc, tranIdColumnName).Equal(pname);
	//	sq.Parameters.Add(pname, transactionId);

	//	return sq;
	//}

	//private SelectQuery GenerateSelectDatasourceQueryIfDifference(int transactionId, Datasource datasource)
	//{
	//	if (datasource == null) throw new ArgumentNullException();
	//	if (datasource.Destination == null) throw new ArgumentNullException();
	//	var dest = datasource.Destination;

	//	var previousq = GenerateSelectPreviousData(transactionId, datasource);

	//	//WITH
	//	//_previous as (select previous data)
	//	//_current as (select current data)
	//	var sq = new SelectQuery();
	//	var ctePrevious = sq.With(previousq).As("_previous");
	//	var cteCurrent = sq.With(new SelectQuery(datasource.Query)).As("_current");

	//	//FROM _previous AS p
	//	//LEFT JOIN _current AS c ON p.key = c.key
	//	var (from, p) = sq.From(ctePrevious).As("p");
	//	var c = from.LeftJoin(cteCurrent).As("c").On(p, datasource.KeyColumns);

	//	sq.Select(() =>
	//	{
	//		var exp = new CaseExpression();
	//		exp.When(new ColumnValue(c, datasource.KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
	//		exp.Else(new LiteralValue("false"));
	//		return exp;
	//	}).As("_is_deleted");
	//	dest.ReverseOption.ReversalColumns.ForEach(x => sq.Select(() =>
	//	{
	//		var v = new ColumnValue(p, x);
	//		v.AddOperatableValue("*", new LiteralValue("-1"));
	//		return v;
	//	}).As(x));
	//	sq.Select(p);

	//	//create condition
	//	//removed changed
	//	var ce = new CaseExpression();
	//	ce.When(new ColumnValue(c, datasource.KeyColumns.First()).IsNull()).Then(new LiteralValue("true"));
	//	ce.Else(new LiteralValue("false"));
	//	ValueBase condition = ce;

	//	//value changed
	//	var commonColumns = dest!.GetDifferenceCheckColumns().Where((string x) => x.IsEqualNoCase(cteCurrent.GetColumnNames())).ToList();
	//	commonColumns.ForEach(x =>
	//	{
	//		//CASE WHEN is_changed THEN true ElSE false END
	//		var prevValue = new ColumnValue(p, x);
	//		var currentValue = new ColumnValue(c, x);

	//		var exp = new CaseExpression();
	//		exp.When(prevValue.IsNull().And(currentValue.IsNull())).Then(new LiteralValue("false"));
	//		exp.When(prevValue.IsNull().And(currentValue.IsNotNull())).Then(new LiteralValue("true"));
	//		exp.When(prevValue.IsNotNull().And(currentValue.IsNull())).Then(new LiteralValue("true"));
	//		exp.When(prevValue.Equal(currentValue)).Then(new LiteralValue("false"));
	//		exp.When(prevValue.NotEqual(currentValue)).Then(new LiteralValue("false"));

	//		condition = condition.Or(exp);
	//	});
	//	// WHERE (CASE WHEN .. END OR CASE WHEN .. END OR CASE WHEN .. END)
	//	if (condition != null) sq.Where(condition.ToGroup());

	//	return sq;
	//}

	//public CreateTableQuery GenerateCreateBridgeQueryAsDifferent(int transactionId,string bridgeName, Datasource datasource, Func<SelectQuery, SelectQuery>? injector = null)
	//{
	//	if (datasource.Destination == null) throw new NullReferenceException(nameof(Destination));
	//	var dest = datasource.Destination;

	//	if (dest.Table == null) throw new NullReferenceException(nameof(dest.Table));
	//	if (dest.Sequence == null) throw new NullReferenceException(nameof(dest.Sequence));

	//	var q = GenerateSelectDatasourceQueryIfDifference(transactionId, datasource);

	//	var sq = new SelectQuery();
	//	var ds = sq.With(q).As("_datasource");
	//	var (_, d) = sq.From(ds).As("d");

	//	sq.Select(d);

	//	if (injector != null) sq = injector(sq);

	//	return sq.ToCreateTableQuery(bridgeName, isTemporary: true);
	//}

}
