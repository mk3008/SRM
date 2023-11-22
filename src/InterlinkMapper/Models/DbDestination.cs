namespace InterlinkMapper.Models;

public class DbDestination //: IDestination
{
	public long DestinationId { get; set; }

	public DbTable Table { get; set; } = new();

	//public ProcessTable ProcessTable { get; set; } = new();

	public Sequence Sequence { get; set; } = new();

	public string Description { get; set; } = string.Empty;

	public ReversalOption? ReversalOption { get; set; } = null;

	//public DbTableDefinition DeleteRequestTable { get; set; } = new();

	//public DbTableDefinition ValidateRequestTable { get; set; } = new();

	//public List<DbCommonTableExtension> DbCommonTableExtensions { get; set; } = new();
}
