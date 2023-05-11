namespace InterlinkMapper;

public class FlipOption
{
	public DbTableDefinition FlipTable { get; set; } = new();

	public List<string> ReversalColumns { get; set; } = new();

	public List<string> ExcludedColumns { get; set; } = new();

	public string FlipIdColumn { get; set; } = string.Empty;
}
