namespace InterlinkMapper.Data;

public interface IDatasource
{
	IDestination Destination { get; }

	string Query { get; }

	string DatasourceName { get; }

	DbTableDefinition KeyMapTable { get; }

	DbTableDefinition RelationMapTable { get; }

	DbTableDefinition HoldTable { get; }

	DbTableDefinition RequestTable { get; }

	bool IsSequence { get; }

	List<string> KeyColumns { get; }
}

public static class IDestinationExtension
{
	public static bool HasKeyMapTable(this IDatasource source) => (string.IsNullOrEmpty(source.KeyMapTable.GetTableFullName()) ? false : true);

	public static bool HasRelationMapTable(this IDatasource source) => (string.IsNullOrEmpty(source.RelationMapTable.GetTableFullName()) ? false : true);

	public static bool HasHoldTable(this IDatasource source) => (string.IsNullOrEmpty(source.HoldTable.GetTableFullName()) ? false : true);

	public static bool HasRequestTable(this IDatasource source) => (string.IsNullOrEmpty(source.RequestTable.GetTableFullName()) ? false : true);
}
