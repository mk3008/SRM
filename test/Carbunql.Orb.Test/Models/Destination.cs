namespace Carbunql.Orb.Test.Models;

public class Destination
{
	public long? DestinationId { get; set; }

	public required string DestinationTableName { get; set; }

	public string Description { get; set; } = string.Empty;

	public required DbTable DbTable { get; set; }

	public required Sequence Sequence { get; set; }

	public ValidateOption? ValidateOption { get; set; }

	public DeleteOption? DeleteOption { get; set; }

	public FlipOption? FlipOption { get; set; }
}
