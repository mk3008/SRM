using InterlinkMapper.Materializer;
using System.Data;

namespace InterlinkMapper.Models;

public class DbDatasource //: IDatasource
{
	public long DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public DbDestination Destination { get; set; } = null!;

	public string Query { get; set; } = string.Empty;

	public string KeyName { get; set; } = string.Empty;

	//public DbTableDefinition KeymapTable { get; set; } = new();

	//public DbTableDefinition RelationmapTable { get; set; } = new();

	//public DbTableDefinition ForwardRequestTable { get; set; } = new();

	//public DbTableDefinition ValidateRequestTable { get; set; } = new();

	//public bool IsSupportSequenceTransfer { get; set; } = false;

	public List<KeyColumn> KeyColumns { get; set; } = new();

	//public string HoldJudgementColumnName { get; set; } = string.Empty;

	public SelectQuery ToSelectQuery()
	{
		var sq = new SelectQuery(Query);
		sq.AddComment("raw data source");
		return sq;
	}
}

