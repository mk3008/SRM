namespace InterlinkMapper;

public class ReversalOption
{
	public DbTableDefinition RequestTable { get; set; } = new();

	public List<string> ReversalColumns { get; set; } = new();

	public List<string> ExcludedColumns { get; set; } = new();

	public string RequestIdColumn { get; set; } = string.Empty;
	//public string FlipIdColumn { get; set; } = string.Empty;
}
