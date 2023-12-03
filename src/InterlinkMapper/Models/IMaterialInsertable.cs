using InterlinkMapper.Materializer;

namespace InterlinkMapper.Models;

public interface IMaterialInsertable
{
	DbTableDefinition Definition { get; set; }
}

public static class IMaterialInsertableExtension
{
	public static InsertQuery CreateInsertQuery(this IMaterialInsertable source, MaterializeResult datasourceMaterial)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(datasourceMaterial.SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(source.Definition.Columns);

		return sq.ToInsertQuery(source.Definition.TableFullName);
	}
}