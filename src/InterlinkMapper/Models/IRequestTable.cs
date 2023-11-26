namespace InterlinkMapper.Models;

public interface IRequestTable
{
	DbTableDefinition Definition { get; set; }
}

public static class IRequestTableExtension
{
	public static SelectQuery ToSelectQuery(this IRequestTable source)
	{
		var table = source.Definition.TableFullName;
		var columns = source.Definition.Columns.ToList();

		var sq = new SelectQuery();
		var (_, r) = sq.From(table).As("r");
		columns.ForEach(column => sq.Select(r, column));

		return sq;
	}
}
