﻿namespace InterlinkMapper.Models;

public class DbTableConfig
{
	public string ControlTableSchemaName { get; set; } = string.Empty;

	//public string InterlinkTransactionTableName { get; set; } = "interlink_transaction";

	//public string InterlinkTransactionIdColumn { get; set; } = "interlink_transaction_id";

	//public string InterlinkProcessTableName { get; set; } = "interlink_process";

	//public string InterlinkProcessIdColumn { get; set; } = "interlink_process_id";

	//public string InterlinkDestinationTableName { get; set; } = "interlink_destination";

	//public string InterlinkDestinationIdColumn { get; set; } = "interlink_destination_id";

	//public string TableNameColumn { get; set; } = "table_name";

	//public string TableDefinitionColumn { get; set; } = "table_definition";

	//public string SequenceDefinitionColumn { get; set; } = "sequence_definition";

	//public string ReverseDefinitionColumn { get; set; } = "reverse_definition";

	//public string InterlinkDatasourceTableName { get; set; } = "interlink_datasource";

	//public string InterlinkDatasourceIdColumn { get; set; } = "interlink_datasource_id";

	//public string InterlinkDatasourceColumn { get; set; } = "datasource_name";

	//public string InterlinkDatasourceNameColumn { get; set; } = "datasource_name";

	//public string KeyDefinitionColumn { get; set; } = "key_columns";

	//public string ArgumentColumn { get; set; } = "argument";

	//public string DescriptionColumn { get; set; } = "description";

	//public string QueryColumn { get; set; } = "query";

	//public string KeyNameColumn { get; set; } = "key_name";

	//public string ServiceNameColumn { get; set; } = "service_name";

	//public string ActionNameColumn { get; set; } = "action_name";

	public string RemarksColumn { get; set; } = "interlink_remarks";

	public string CreateTimestampColumn { get; set; } = "created_at";

	public string UpdateTimestampColumn { get; set; } = "updated_at";

	//public string InsertCountColumn { get; set; } = "insert_count";

	public string KeyMapTableNameColumn { get; set; } = "interlink_key_map";

	public string KeyRelationTableNameColumn { get; set; } = "interlink_key_relation";

	public string RelationTableNameFormat { get; set; } = "{0}__relation";

	public string KeyMapTableNameFormat { get; set; } = "{0}__key_m_{1}";

	public string KeyRelationTableNameFormat { get; set; } = "{0}__key_r_{1}";

	public string RootIdColumnFormat { get; set; } = "root__{0}";

	public string OriginIdColumnFormat { get; set; } = "origin__{0}";

	public string RequestIdColumnFormat { get; set; } = "{0}_id";

	public string InsertRequestTableNameFormat { get; set; } = "{0}__req_i_{1}";

	public string ReverseRequestTableNameFormat { get; set; } = "{0}__req_reverse";

	public string ValidateRequestTableNameFormat { get; set; } = "{0}__req_v_{1}";
}
