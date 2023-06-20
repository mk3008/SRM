namespace Carbunql.Orb.Test.DBTestModels;

public class DeleteOption
{
	public required DbTableDefinition RequestTable { get; set; }

	public string RequestIdColumn => RequestTable.GetColumnName("RequestId");
}
