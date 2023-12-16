namespace InterlinkMapper.Models;

public class InterlinkDatasource
{
	public long InterlinkDatasourceId { get; set; }

	public required string DatasourceName { get; set; }

	public string Description { get; set; } = string.Empty;

	public required InterlinkDestination Destination { get; set; }

	public required string Query { get; set; }

	public required string KeyName { get; set; }

	public required List<KeyColumn> KeyColumns { get; set; }

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery(Query);
		sq.AddComment("raw data source");
		return sq;
	}
}
