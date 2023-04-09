namespace InterlinkMapper;

public class DbAccessor
{

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
}
