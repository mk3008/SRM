namespace Carbunql.Orb.Mapping;

public class TypeMap
{
	public required string TableAlias { get; set; }

	public required Type Type { get; set; }

	public RelationMap? RelationMap { get; set; }

	public List<ColumnMap> ColumnMaps { get; set; } = new();
}
