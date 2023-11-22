namespace InterlinkMapper.Models;

public class ReversalRequestTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RequestIdColumn { get; set; } = string.Empty;

	public string DestinationSequenceColumn { get; set; } = string.Empty;
}
