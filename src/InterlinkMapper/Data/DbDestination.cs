namespace InterlinkMapper.Data;

public class DbDestination : IDestination
{
	public int DestinationId { get; set; }

	public DbTable Table { get; set; } = new();

	public DbTable ProcessMap { get; set; } = new();

	public Sequence Sequence { get; set; } = new();

	public string Description { get; set; } = string.Empty;

	public ReverseOption ReverseOption { get; set; } = new();
}
