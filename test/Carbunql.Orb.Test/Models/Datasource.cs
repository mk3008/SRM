namespace Carbunql.Orb.Test.Models;

public class Datasource
{
	public long? DatasourceId { get; set; }

	public required string DatasourceName { get; set; }

	public string Description { get; set; } = string.Empty;

	public required Destination Destination { get; set; }

	public required string Query { get; set; }

	public DbTableDefinition? KeymapTable { get; set; }

	public DbTableDefinition? RelationmapTable { get; set; }

	public DbTableDefinition? ForwardRequestTable { get; set; }

	public DbTableDefinition? ValidateRequestTable { get; set; }

	public List<string> KeyColumnNames { get; set; } = new();

	public string HoldJudgementColumnName { get; set; } = string.Empty;
}
