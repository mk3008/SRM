using InterlinkMapper.Materializer;
using RedOrb;

namespace InterlinkMapper.Models;

public class InterlinkDestination
{
	public long InterlinkDestinationId { get; set; }

	public required DbTable Table { get; set; }

	public required Sequence Sequence { get; set; }

	public string Description { get; set; } = string.Empty;

	public required ReverseOption ReverseOption { get; set; }

	public bool AllowReverse => ReverseOption.ReverseColumns.Any();

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery();
		sq.AddComment("destination");
		var (f, d) = sq.From(Table.TableFullName).As("d");
		Table.ColumnNames.ForEach(x => sq.Select(d, x));
		return sq;
	}

	public InsertQuery CreateInsertQueryFrom(MaterializeResult datasourceMaterial)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(datasourceMaterial.SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(Table.ColumnNames);

		return sq.ToInsertQuery(Table.TableFullName);
	}
}
