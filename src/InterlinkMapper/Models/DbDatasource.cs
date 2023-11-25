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

	public InsertQuery ToDestinationInsertQuery(MaterializeResult datasourceMaterial)
	{
		var sq = (SelectQuery)datasourceMaterial.SelectQuery.DeepCopy();

		// Exclude from selection if it does not exist in the destination column
		var columns = sq.GetSelectableItems().ToList();
		foreach (var item in columns.Where(x => !Destination.Table.Columns.Contains(x.Alias, StringComparer.OrdinalIgnoreCase)))
		{
			sq.SelectClause!.Remove(item);
		}

		return sq.ToInsertQuery(Destination.Table.GetTableFullName());
	}
}

