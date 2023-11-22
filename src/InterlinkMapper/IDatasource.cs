namespace InterlinkMapper;

//public interface IDatasource
//{
//	IDestination Destination { get; }

//	string Query { get; }

//	long DatasourceId { get; }

//	string DatasourceName { get; }

//	//DbTableDefinition KeymapTable { get; }

//	//DbTableDefinition RelationmapTable { get; }

//	//DbTableDefinition ForwardRequestTable { get; }

//	//DbTableDefinition ValidateRequestTable { get; }

//	bool IsSupportSequenceTransfer { get; }

//	List<string> KeyColumns { get; }

//	//string HoldJudgementColumnName { get; }
//}

public static class IDatasourceExtension
{
	public static bool HasKeyMapTable(this IDatasource source) => string.IsNullOrEmpty(source.KeymapTable.GetTableFullName()) ? false : true;

	public static bool HasRelationMapTable(this IDatasource source) => string.IsNullOrEmpty(source.RelationmapTable.GetTableFullName()) ? false : true;

	public static bool HasForwardRequestTable(this IDatasource source) => string.IsNullOrEmpty(source.ForwardRequestTable.GetTableFullName()) ? false : true;

	public static bool HasValidateRequestTable(this IDatasource source) => string.IsNullOrEmpty(source.ValidateRequestTable.GetTableFullName()) ? false : true;

	public static SelectQuery ToSelectDatasourceQuery(this IDatasource source, string alias = "d")
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(new SelectQuery(source.Query)).As("d");
		sq.Select(d);
		return sq;
	}

	public static SelectQuery ToSelectQuery(this IDatasource source)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(new SelectQuery(source.Query)).As("d");
		sq.Select(d);
		return sq;
	}

	public static SelectQuery ToSelectQueryAsNotTransfered(this IDatasource source)
	{
		var sq = source.ToSelectQuery();

		sq.SelectForwardRequestId(source);

		//If there is no keymap, select all (assuming proper filtering in the datasource query).
		var keymapTable = source.KeymapTable.GetTableFullName();
		if (string.IsNullOrEmpty(keymapTable) || !source.KeyColumns.Any()) return sq;

		//If Sequence transfer is not supported, check individual keys.
		sq.WhereKeymapIsNull(source);

		return sq;
	}

	//private static void AddConditionAsGreaterThanTransferedSequence(IDatasource ds, SelectQuery query)
	//{
	//	//WHERE (SELECT COALESCE(MAX(datasourceSeqColumn), 0) FROM keymapTable) < datasourceTable.datasourceSeqColumn
	//	var from = query.FromClause;
	//	if (from == null) throw new NullReferenceException(nameof(from));
	//	var dsTable = from.Root;

	//	var seqColumn = ds.KeyColumns.First();

	//	var sq = new SelectQuery();
	//	sq.From(ds.KeymapTable.GetTableFullName()).As("m");
	//	sq.Select($"coalesce(max(m.{seqColumn}),0)").As(seqColumn);
	//	var v = sq.ToValue();
	//	v.AddOperatableValue("<", new ColumnValue(dsTable, seqColumn));

	//	query.Where(v);
	//}

	private static void WhereKeymapIsNull(this SelectQuery source, IDatasource ds)
	{
		// SELECT d.key1, d.key2, ...
		// FROM datasources AS d
		// LEFT JOIN map AS m ON d.key1 = m.key1 AND d.key2 = m.key2
		// WHERE m.key1 IS NULL
		var from = source.FromClause;
		if (from == null) throw new NullReferenceException(nameof(from));
		var d = from.Root;

		var keymapTable = ds.KeymapTable.GetTableFullName();
		if (string.IsNullOrEmpty(keymapTable)) throw new NullReferenceException(nameof(keymapTable));

		var m = from.LeftJoin(keymapTable).As("m").On(d, ds.KeyColumns);

		foreach (var key in ds.KeyColumns)
		{
			if (source.SelectClause!.Where(x => x.Alias == key).Any()) continue;
			source.Select(d, key);
		}

		source.Where(m, ds.KeyColumns.First()).IsNull();
	}

	private static void SelectForwardRequestId(this SelectQuery source, IDatasource ds)
	{
		var seq = ds.ForwardRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

		var table = source.FromClause!.Root;
		var req = source.FromClause!.InnerJoin(ds.ForwardRequestTable.GetTableFullName()).As("req").On(table, ds.KeyColumns);
		source.Select(req, seq.ColumnName);
	}
}
