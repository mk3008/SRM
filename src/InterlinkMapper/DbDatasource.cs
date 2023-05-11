namespace InterlinkMapper;

public class DbDatasource : IDatasource
{
	public int DatasourceId { get; set; }

	public string DatasourceName { get; set; } = string.Empty;

	public string Description { get; set; } = string.Empty;

	public IDestination Destination { get; set; } = new DbDestination();

	public string Query { get; set; } = string.Empty;

	public DbTableDefinition KeyMapTable { get; set; } = new();

	public DbTableDefinition RelationMapTable { get; set; } = new();

	public DbTableDefinition ForwardRequestTable { get; set; } = new();

	public DbTableDefinition ValidateRequestTable { get; set; } = new();

	public bool IsSupportSequenceTransfer { get; set; } = false;

	public List<string> KeyColumns { get; set; } = new();

	public string HoldJudgementColumnName { get; set; } = string.Empty;
}