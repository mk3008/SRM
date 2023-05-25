namespace InterlinkMapper;

public class ProcessTable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RootIdColumnName { get; set; } = string.Empty;

	public string SourceIdColumnName { get; set; } = string.Empty;

	public string FlipColumnName { get; set; } = string.Empty;
}
