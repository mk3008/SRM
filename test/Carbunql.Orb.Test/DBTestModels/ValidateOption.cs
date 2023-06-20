namespace Carbunql.Orb.Test.DBTestModels;

public class ValidateOption
{
	public required DbTableDefinition RequestTable { get; set; }

	public string RequestIdColumn => RequestTable.GetColumnName("RequestId")!;
}
