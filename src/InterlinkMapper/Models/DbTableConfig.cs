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

	public string KeyMapTableNameFormat { get; set; } = "{0}__km_{1}";

	public string KeyRelationTableNameFormat { get; set; } = "{0}__kr_{1}";

	public string ReverseTableNameFormat { get; set; } = "{0}__reverse";

	public string InsertRequestTableNameFormat { get; set; } = "{0}__ri_{1}";

	public string ValidateRequestTableNameFormat { get; set; } = "{0}__rv_{1}";

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

	public string RemarksColumn { get; set; } = "interlink__remarks";

	public string TimestampColumn { get; set; } = "created_at";

	public string InsertCountColumn { get; set; } = "insert_count";

	public string KeyMapTableNameColumn { get; set; } = "key_map";

	public string KeyRelationTableNameColumn { get; set; } = "key_relation";

	public string RootIdColumnFormat { get; set; } = "root__{0}";

	public string OriginIdColumnFormat { get; set; } = "origin__{0}";

	public string RequestIdColumnFormat { get; set; } = "{0}_id";
}
