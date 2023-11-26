namespace InterlinkMapper.Models;

public class ReverseRequestTable : IRequestTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RequestIdColumn { get; set; } = string.Empty;

	public string DestinationSequenceColumn { get; set; } = string.Empty;

	//public SelectQuery ToSelectQuery()
	//{
	//	var table = Definition.TableFullName;
	//	var columns = Definition.Columns.ToList();

	//	var sq = new SelectQuery();
	//	var (_, r) = sq.From(table).As("r");
	//	columns.ForEach(column => sq.Select(r, column));

	//	return sq;
	//}
}
