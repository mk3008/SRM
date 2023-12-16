using InterlinkMapper.Materializer;

namespace InterlinkMapper.Models;

public class InterlinkDestination
{
	public long InterlinkDestinationId { get; set; }

	public DbTable Table { get; set; } = new();

	public Sequence Sequence { get; set; } = new();

	public string Description { get; set; } = string.Empty;

	public ReverseOption ReverseOption { get; set; } = new();

	public bool AllowReverse => ReverseOption.ReverseColumns.Any();

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery();
		sq.AddComment("destination");
		var (f, d) = sq.From(Table.GetTableFullName()).As("d");
		Table.Columns.ForEach(x => sq.Select(d, x));
		return sq;
	}

	public InsertQuery CreateInsertQueryFrom(MaterializeResult datasourceMaterial)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(datasourceMaterial.SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(Table.Columns);

		return sq.ToInsertQuery(Table.GetTableFullName());
	}
}
