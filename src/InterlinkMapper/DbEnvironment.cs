namespace InterlinkMapper;

public class DbEnvironment
{
	public DbTableDefinition TransactionTable { get; set; } = new();

	public DbTableDefinition ProcessnTable { get; set; } = new();

	public string TransactionIdColumn { get; set; } = string.Empty;

	public string ProcessIdColumn { get; set; } = string.Empty;

	public string DestinationTableNameColumn { get; set; } = string.Empty;

	public string DatasourceNameColumn { get; set; } = string.Empty;

	public string KeymapTableNameColumn { get; set; } = string.Empty;

	public string RelationmapTableNameColumn { get; set; } = string.Empty;

	public string TimestampColumn { get; set; } = string.Empty;

	public string PlaceHolderIdentifer { get; set; } = string.Empty;
}
