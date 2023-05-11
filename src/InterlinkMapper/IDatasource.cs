using Carbunql;
using Carbunql.Building;

namespace InterlinkMapper;

public interface IDatasource
{
	IDestination Destination { get; }

	string Query { get; }

	string DatasourceName { get; }

	DbTableDefinition KeyMapTable { get; }

	DbTableDefinition RelationMapTable { get; }

	DbTableDefinition ForwardRequestTable { get; }

	DbTableDefinition ValidateRequestTable { get; }

	bool IsSupportSequenceTransfer { get; }

	List<string> KeyColumns { get; }

	string HoldJudgementColumnName { get; }
}

public static class IDestinationExtension
{
	public static bool HasKeyMapTable(this IDatasource source) => string.IsNullOrEmpty(source.KeyMapTable.GetTableFullName()) ? false : true;

	public static bool HasRelationMapTable(this IDatasource source) => string.IsNullOrEmpty(source.RelationMapTable.GetTableFullName()) ? false : true;

	public static bool HasForwardRequestTable(this IDatasource source) => string.IsNullOrEmpty(source.ForwardRequestTable.GetTableFullName()) ? false : true;

	public static bool HasValidateRequestTable(this IDatasource source) => string.IsNullOrEmpty(source.ValidateRequestTable.GetTableFullName()) ? false : true;

	public static SelectQuery ToSelectDatasourceQuery(this IDatasource source, string alias = "d")
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(new SelectQuery(source.Query)).As("d");
		sq.Select(d);
		return sq;
	}
}
