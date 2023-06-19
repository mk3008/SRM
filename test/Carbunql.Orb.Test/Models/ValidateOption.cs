namespace Carbunql.Orb.Test.Models;

public class ValidateOption
{
	public required DbTableDefinition RequestTable { get; set; }

	public string RequestIdColumn => RequestTable.GetColumnName("RequestId")!;
}
