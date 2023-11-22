namespace InterlinkMapper;

public class DestinationTableDefinition : DbTableDefinition
{
	public string DestinationIdColumnName { get; set; } = string.Empty;

	public string DestinationNameColumnName { get; set; } = string.Empty;

	public string TableColumnName { get; set; } = string.Empty;

	public string DescriptionColumnName { get; set; } = string.Empty;

	public string SequenceColumnName { get; set; } = string.Empty;

	public string FlipOptionColumnName { get; set; } = string.Empty;

	public string DeleteRequestTableColumnName { get; set; } = string.Empty;

	public string ValidateRequestTableColumnName { get; set; } = string.Empty;

	public string DbCommonTableExtensionsColumnName { get; set; } = string.Empty;
}
