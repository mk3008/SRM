namespace InterlinkMapper.Models;

public class ReverseTable : IMaterialInsertable
{
	public DbTableDefinition Definition { get; set; } = new();

	public string RootIdColumn { get; set; } = string.Empty;

	public string OriginIdColumn { get; set; } = string.Empty;

	public string ReverseIdColumn { get; set; } = string.Empty;

	public string RemarksColumn { get; set; } = string.Empty;
}
