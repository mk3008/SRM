namespace Carbunql.Orb;

public class DbTable : IDbTable
{
	public string SchemaName { get; set; } = string.Empty;

	public required string TableName { get; set; }

	public List<string> Columns { get; init; } = new();

	IEnumerable<string> IDbTable.Columns => Columns;
}