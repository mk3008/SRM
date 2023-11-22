namespace InterlinkMapper;

public class DatasourceTableDefinition : DbTableDefinition
{
	public string DatasourceIdColumnName { get; set; } = string.Empty;

	public string DatasourceNameColumnName { get; set; } = string.Empty;

	public string QueryColumnName { get; set; } = string.Empty;

	public string DescriptionColumnName { get; set; } = string.Empty;

	public string KeymapDefinitionColumnName { get; set; } = string.Empty;

	public string RelationmapDefinitionColumnName { get; set; } = string.Empty;

	public string ForwardRequestTableDefinitionColumnName { get; set; } = string.Empty;

	public string ValidatedRequestTableDefinitionColumnName { get; set; } = string.Empty;

	public string IsSupportSequenceTransferColumnName { get; set; } = string.Empty;

	public string KeyColumnsColumnName { get; set; } = string.Empty;

	public string HoldJudgementColumnName { get; set; } = string.Empty;
}