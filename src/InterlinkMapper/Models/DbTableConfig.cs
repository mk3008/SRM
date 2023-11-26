namespace InterlinkMapper.Models;

public class DbTableConfig
{
	//public DbTableDefinition TransactionTable { get; set; } = new();

	//public DbTableDefinition ProcessTable { get; set; } = new();

	//public DbTableDefinition ProcessResultTable { get; set; } = new();

	//public DbTableDefinition DestinationTable { get; set; } = new();

	//public DbTableDefinition DatasourceTable { get; set; } = new();

	public string ControlTableSchemaName { get; set; } = string.Empty;

	public string TransactionTableName { get; set; } = "interlink_transaction";

	public string ProcessTableName { get; set; } = "interlink_process";

	public string RelationTableNameFormat { get; set; } = "{0}__relation";

	public string KeymapTableNameFormat { get; set; } = "{0}__m_{1}";

	public string ReversalTableNameFormat { get; set; } = "{0}__reversal";

	public string InsertRequestTableNameFormat { get; set; } = "{0}__r_{1}";

	public string ValidateRequestTableNameFormat { get; set; } = "{0}__r__validation";

	public string ReverseRequestTableNameFormat { get; set; } = "{0}__r__reverse";

	public string TransactionIdColumn { get; set; } = "interlink__transaction_id";

	public string ProcessIdColumn { get; set; } = "interlink__process_id";

	public string DestinationIdColumn { get; set; } = "destination_id";

	public string DatasourceIdColumn { get; set; } = "datasource_id";

	//public string DestinationTableNameColumn { get; set; } = string.Empty;

	//public string DatasourceNameColumn { get; set; } = string.Empty;

	//public string DatasourceKeyColumn { get; set; } = string.Empty;


	//public string RelationmapTableNameColumn { get; set; } = string.Empty;

	public string ArgumentColumn { get; set; } = "argument";

	//public string MemberNameColumn { get; set; } = string.Empty;

	//public string TableNameColumn { get; set; } = string.Empty;

	public string ActionNameColumn { get; set; } = "action";

	public string TimestampColumn { get; set; } = "created_at";

	public string InsertCountColumn { get; set; } = "insert_count";

	public string KeymapTableNameColumn { get; set; } = "keymap_name";

	public string OriginIdColumnFormat { get; set; } = "origin__{0}";

	public string RequestIdColumnFormat { get; set; } = "{0}_id";


}
