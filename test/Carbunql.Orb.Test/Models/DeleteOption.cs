namespace Carbunql.Orb.Test.Models;

public class DeleteOption
{
	public required DbTableDefinition RequestTable { get; set; }

	public string RequestIdColumn => RequestTable.GetColumnName("RequestId");
}
