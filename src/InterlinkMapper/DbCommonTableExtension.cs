namespace InterlinkMapper;

public class DbCommonTableExtension
{
	public int CommonTableExtensionId { get; set; }

	public string CommanTableExtensionName { get; set; } = string.Empty;

	public string Query { get; set; } = string.Empty;

	public string ValueColumn { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public string DbFunction { get; set; } = "greatest";
}
