using Carbunql;
using Carbunql.Building;

namespace InterlinkMapper.Data;

public class DbTable
{
	public string SchemaName { get; set; } = string.Empty;

	public string TableName { get; set; } = string.Empty;

	//public abstract IEnumerable<string> GetPrimaryKeyColumns();

	//public abstract string? GetSequenceColumn();

	//public abstract IEnumerable<string> GetUniqueKeyColumns();

	public List<string> Columns { get; set; } = new();

	public string TableFullName => string.IsNullOrEmpty(SchemaName) ? TableName : SchemaName + "." + TableName;

	//public InsertQuery ConvertToInsertQuery(SelectQuery query)
	//{
	//	if (string.IsNullOrEmpty(TableName)) throw new NullReferenceException(nameof(TableName));

	//	var sq = new SelectQuery();
	//	var (_, q) = sq.From(query).As("_iq");
	//	sq.Select(q);
	//	sq.SelectClause!.FilterInColumns(GetColumns());

	//	return sq.ToInsertQuery(TableFullName);
	//}
}