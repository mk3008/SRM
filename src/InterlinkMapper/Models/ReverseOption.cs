namespace InterlinkMapper.Models;

public class ReverseOption
{
	public List<string> ReverseColumns { get; set; } = new();

	public List<string> ExcludedColumns { get; set; } = new();
}
