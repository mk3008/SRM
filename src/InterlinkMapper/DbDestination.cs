namespace InterlinkMapper;

public class DbDestination : IDestination
{
	public int DestinationId { get; set; }

	public DbTable Table { get; set; } = new();

	public DbTableDefinition ProcessTable { get; set; } = new();

	public Sequence Sequence { get; set; } = new();

	public string Description { get; set; } = string.Empty;

	public FlipOption FlipOption { get; set; } = new();

	public DbTableDefinition ValidateRequestTable { get; set; } = new();
}
