namespace InterlinkMapper.Models;

public class ValidateRequestTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RequestIdColumn { get; set; } = string.Empty;

	public string DestinationSequenceColumn { get; set; } = string.Empty;
}
