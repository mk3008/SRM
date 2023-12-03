namespace InterlinkMapper.Models;

public class DbDatasource
{
	public long DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public DbDestination Destination { get; set; } = null!;

	public string Query { get; set; } = string.Empty;

	public string KeyName { get; set; } = string.Empty;

	public List<KeyColumn> KeyColumns { get; set; } = new();

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery(Query);
		sq.AddComment("raw data source");
		return sq;
	}
}
