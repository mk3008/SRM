namespace InterlinkMapper.Models;

public class ReversalTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string OriginIdColumn { get; set; } = string.Empty;

	public string ReversalIdColumn { get; set; } = string.Empty;
}

