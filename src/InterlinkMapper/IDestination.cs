namespace InterlinkMapper;

//public interface IDestination
//{
//	long DestinationId { get; }

//	DbTable Table { get; set; }

//	//ProcessTable ProcessTable { get; set; }

//	//DbTableDefinition ValidateRequestTable { get; }

//	//DbTableDefinition DeleteRequestTable { get; }

//	Sequence Sequence { get; set; }

//	ReversalOption? ReversalOption { get; set; }
//}

public static class DestinationExtension
{
	public static bool HasProcessTable(this IDestination source) => string.IsNullOrEmpty(source.ProcessTable.Definition.GetTableFullName()) ? false : true;

	public static bool HasFlipTable(this IDestination source) => string.IsNullOrEmpty(source.ReversalOption.RequestTable.GetTableFullName()) ? false : true;

	public static bool HasDeleteRequestTable(this IDestination source) => string.IsNullOrEmpty(source.DeleteRequestTable.GetTableFullName()) ? false : true;

	public static bool HasValidateRequestTable(this IDestination source) => string.IsNullOrEmpty(source.ValidateRequestTable.GetTableFullName()) ? false : true;

	public static List<string> GetDifferenceCheckColumns(this IDestination source)
	{
		var q = source.Table.Columns.Where(x => !x.IsEqualNoCase(source.Sequence.Column));
		q = q.Where(x => !x.IsEqualNoCase(source.ReversalOption.ExcludedColumns));
		return q.ToList();
	}

	public static SelectQuery ToSelectQueryWithoutKey(this IDestination source, string alias = "d")
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(source.Table.GetTableFullName()).As("d");
		source.Table.Columns.Where(x => !x.IsEqualNoCase(source.Sequence.Column)).ToList().ForEach(x => sq.Select(d, x));
		return sq;
	}

	/// <summary>
	/// SELECT columns FROM destination WHERE flip_requests
	/// </summary>
	/// <param name="destination"></param>
	/// <param name="keymapTable"></param>
	/// <returns></returns>
	public static SelectQuery ToSelectQueryAsFlip(this IDestination destination)
	{
		var sq = destination.ToSelectQueryWithoutKey();

		sq.SelectFlips(destination);

		sq.SelectSourceId(destination);
		sq.SelectRootSourceId(destination);
		sq.SelectFlipRequestId(destination);

		return sq;
	}

	private static void SelectSourceId(this SelectQuery source, IDestination destination)
	{
		var table = source.FromClause!.Root;
		source.Select(table, destination.Sequence.Column).As(destination.ProcessTable.DatasourceIdColumn);
	}

	private static void SelectRootSourceId(this SelectQuery source, IDestination destination)
	{
		var table = source.FromClause!.Root;
		var proc = source.FromClause!.InnerJoin(destination.ProcessTable.Definition.GetTableFullName()).As("proc").On(table, destination.Sequence.Column);
		source.Select(proc, destination.ProcessTable.RootIdColumnName);
	}

	private static void SelectFlips(this SelectQuery source, IDestination destination)
	{
		destination.ReversalOption.ReversalColumns.ForEach(x =>
		{
			var column = source.SelectClause!.Where(c => c.Alias.IsEqualNoCase(x)).FirstOrDefault();
			if (column == null) return;
			source.SelectClause!.Remove(column);
			source.Select(column.Value.ToCommand().CommandText + " * -1").As(x);
		});
	}

	private static void SelectFlipRequestId(this SelectQuery source, IDestination destination)
	{
		var table = source.FromClause!.Root;
		var req = source.FromClause!.InnerJoin(destination.ReversalOption.RequestTable.GetTableFullName()).As("req").On(table, destination.Sequence.Column);
		source.Select(req, destination.ReversalOption.RequestIdColumn);
	}

	/// <summary>
	/// Get the sequence.
	/// </summary>
	/// <param name="source"></param>
	/// <param name="destionation"></param>
	public static void SelectSequenceColumn(this SelectQuery source, IDestination destionation)
	{
		source.Select(destionation.Sequence.Command).As(destionation.Sequence.Column);
	}

	/// <summary>
	/// Get the sequence.
	/// However, if the transfer conditions are not met, NULL will be obtained.
	/// </summary>
	/// <param name="source"></param>
	/// <param name="datasource"></param>
	public static void SelectSequenceOrNullColumn(this SelectQuery source, IDatasource datasource)
	{
		if (source.SelectClause!.Where(x => x.Alias.IsEqualNoCase(datasource.HoldJudgementColumnName)).Any())
		{
			source.SelectSequenceColumn(datasource.Destination);
			return;
		}

		var seq = datasource.Destination.Sequence;

		var datasourceTable = source.FromClause!.Root;
		source.Select(() =>
		{
			var c = new CaseExpression();
			c.When(new ColumnValue(datasourceTable, datasource.HoldJudgementColumnName).False()).Then(new LiteralValue(seq.Command));
			return c;
		}).As(seq.Column);
	}
}
