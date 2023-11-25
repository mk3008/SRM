namespace InterlinkMapper.Models;

public class ReverseOption
{
	//public DbTableDefinition RequestTable { get; set; } = new();

	public List<string> ReverseColumns { get; set; } = new();

	public List<string> ExcludedColumns { get; set; } = new();

	//public string RequestIdColumn { get; set; } = string.Empty;
	//public string FlipIdColumn { get; set; } = string.Empty;
}
