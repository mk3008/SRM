namespace Carbunql.Orb.Test.DBTestModels;

public static class DefinitionRepository
{
	public static DbTableDefinition<Destination> GetDestinationTableDefinition()
	{
		var t = new DbTableDefinition<Destination>()
		{
			SchemaName = "public",
			TableName = "destinations",
			ColumnDefinitions = new()
			{
				new () { Identifer = "DestinationId", ColumnName = "destination_id", ColumnType = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DestinationTableName", ColumnName = "destination_table_name", ColumnType = "text" },
				new () { Identifer = "Description", ColumnName = "description", ColumnType = "text" },
				new () { Identifer = "DbTable", ColumnName = "db_table_text", ColumnType = "text" },
				new () { Identifer = "Sequence", ColumnName = "sequence_text", ColumnType = "text" },
				new () { Identifer = "ValidateOption", IsNullable = true,  ColumnName = "validate_option", ColumnType = "text" },
				new () { Identifer = "FlipOption", IsNullable = true, ColumnName = "flip_option", ColumnType = "text" },
				new () { Identifer = "DeleteOption", IsNullable = true, ColumnName = "delete_option", ColumnType = "text" },
				new () { ColumnName = "created_at", ColumnType = "timestamp", DefaultValue = "current_timestamp" },
				new () { ColumnName = "updated_at", ColumnType = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { Identifers = new() { "DestinationTableName"}, IsUnique = true },
			}
		};
		return t;
	}

	public static DbTableDefinition<Datasource> GetDatasourceTableDefinition()
	{
		var t = new DbTableDefinition<Datasource>()
		{
			SchemaName = "public",
			TableName = "datasources",
			ColumnDefinitions = new()
			{
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", ColumnType = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DatasourceName", ColumnName = "datasource_name", ColumnType = "text" },
				new () { Identifer = "Description", ColumnName = "description", ColumnType = "text" },
				new () { Identifer = "DestinationId", ColumnName = "destination_id", ColumnType = "int8" },
				new () { Identifer = "Query", ColumnName = "query", ColumnType = "text" },
				new () { Identifer = "KeyColumnNames", ColumnName = "key_columns_text", ColumnType = "text" },
				new () { Identifer = "KeymapTable", IsNullable = true, ColumnName = "keymap_table", ColumnType = "text" },
				new () { Identifer = "RelationmapTable", IsNullable = true, ColumnName = "relationmap_table_text", ColumnType = "text" },
				new () { Identifer = "ForwardRequestTable", IsNullable = true, ColumnName = "forward_request_table_text", ColumnType = "text" },
				new () { Identifer = "ValidateRequestTable", IsNullable = true, ColumnName = "validate_request_table_text", ColumnType = "text" },
				new () { Identifer = "IsEnabled", ColumnName = "is_enabled", ColumnType = "bool" },
				new () { ColumnName = "created_at", ColumnType = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				new () { ColumnName = "updated_at", ColumnType = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.UpdateTimestamp },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { Identifers = new() { "DatasourceName"}, IsUnique = true },
				new DbIndexDefinition() { Identifers = new() { "DestinationId"}},
			}
		};
		return t;
	}

	public static DbTableDefinition GetTransactionTableDefinition()
	{
		var t = new DbTableDefinition()
		{
			SchemaName = "public",
			TableName = "transactions",
			ColumnDefinitions = new()
			{
				new () { Identifer = "TransactionId", ColumnName = "transaction_id", ColumnType = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", ColumnType = "int8" },
				new () { ColumnName = "created_at", ColumnType = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { Identifers = new() { "DatasourceId"}},
			}
		};
		return t;
	}

	public static DbTableDefinition GetProcessTableDefinition()
	{
		var t = new DbTableDefinition()
		{
			SchemaName = "public",
			TableName = "processes",
			ColumnDefinitions = new()
			{
				new () { Identifer = "ProcessId", ColumnName = "process_id", ColumnType = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "TransactionId", ColumnName = "transaction_id", ColumnType = "int8" },
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", ColumnType = "int8" },
				new () { ColumnName = "created_at", ColumnType = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { Identifers = new() { "TransactionId"}},
				new DbIndexDefinition() { Identifers = new() { "DatasourceId"}},
			}
		};
		return t;
	}

	public static DbTableDefinition GetProcessResultTableDefinition()
	{
		var t = new DbTableDefinition()
		{
			SchemaName = "public",
			TableName = "process_results",
			ColumnDefinitions = new()
			{
				new () { ColumnName = "process_result_id", ColumnType = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "ProcessId", ColumnName = "process_id", ColumnType = "int8" },
				new () { Identifer = "FunctionName", ColumnName = "function_name", ColumnType = "text" },
				new () { Identifer = "TableName", ColumnName = "table_name", ColumnType = "text" },
				new () { Identifer = "Action", ColumnName = "action", ColumnType = "text" },
				new () { Identifer = "ResultCount", ColumnName = "result_count", ColumnType = "int8" },
				new () { ColumnName = "created_at", ColumnType = "timestamp", DefaultValue = "clock_timestamp()" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { Identifers = new() { "ProcessId"}},
			}
		};
		return t;
	}
}
