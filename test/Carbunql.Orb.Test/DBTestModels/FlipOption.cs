namespace Carbunql.Orb.Test.DBTestModels;

public class FlipOption
{
	public required DbTableDefinition RequestTable { get; set; }

	public List<string> FlipColumns { get; init; } = new();

	public List<string> ExcludedColumns { get; init; } = new();

	public string RequestIdColumn => RequestTable.GetColumnName("RequestId");
}
