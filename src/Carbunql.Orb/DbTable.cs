namespace Carbunql.Orb;

public class DbTable : IDbTable
{
	public string SchemaName { get; set; } = string.Empty;

	public required string TableName { get; set; }

	public List<string> ColumnNames { get; init; } = new();

	IEnumerable<string> IDbTable.ColumnNames => ColumnNames;
}