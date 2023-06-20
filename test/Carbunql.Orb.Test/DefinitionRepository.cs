using Carbunql.Orb.Test.Models;

namespace Carbunql.Orb.Test;

public static class DefinitionRepository
{
	public static MappableDbTableDefinition GetDestinationTableDefinition()
	{
		var t = new MappableDbTableDefinition()
		{
			SchemaName = "public",
			TableName = "destinations",
			Type = typeof(Destination),
			ColumnDefinitions = new()
			{
				new () { Identifer = "DestinationId", ColumnName = "destination_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DestinationTableName", ColumnName = "destination_table_name", TypeName = "text" },
				new () { Identifer = "Description", ColumnName = "description", TypeName = "text" },
				new () { Identifer = "DbTable", ColumnName = "db_table_text", TypeName = "text" },
				new () { Identifer = "Sequence", ColumnName = "sequence_text", TypeName = "text" },
				new () { Identifer = "ValidateOption", AllowNull = true,  ColumnName = "validate_option", TypeName = "text" },
				new () { Identifer = "FlipOption", AllowNull = true, ColumnName = "flip_option", TypeName = "text" },
				new () { Identifer = "DeleteOption", AllowNull = true, ColumnName = "delete_option", TypeName = "text" },
				new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
				new () { ColumnName = "updated_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "DestinationTableName"}, IsUnique = true },
			}
		};
		return t;
	}

	public static MappableDbTableDefinition GetDatasourceTableDefinition()
	{
		var t = new MappableDbTableDefinition()
		{
			SchemaName = "public",
			TableName = "datasources",
			Type = typeof(Datasource),
			ColumnDefinitions = new()
			{
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DatasourceName", ColumnName = "datasource_name", TypeName = "text" },
				new () { Identifer = "Description", ColumnName = "description", TypeName = "text" },
				new () { Identifer = "DestinationId", ColumnName = "destination_id", TypeName = "int8" },
				new () { Identifer = "Query", ColumnName = "query", TypeName = "text" },
				new () { Identifer = "KeyColumnNames", ColumnName = "key_columns_text", TypeName = "text" },
				new () { Identifer = "KeymapTable", AllowNull = true, ColumnName = "keymap_table", TypeName = "text" },
				new () { Identifer = "RelationmapTable", AllowNull = true, ColumnName = "relationmap_table_text", TypeName = "text" },
				new () { Identifer = "ForwardRequestTable", AllowNull = true, ColumnName = "forward_request_table_text", TypeName = "text" },
				new () { Identifer = "ValidateRequestTable", AllowNull = true, ColumnName = "validate_request_table_text", TypeName = "text" },
				new () { Identifer = "IsEnabled", ColumnName = "is_enabled", TypeName = "bool" },
				new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				new () { ColumnName = "updated_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.UpdateTimestamp },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "DatasourceName"}, IsUnique = true },
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "DestinationId"}},
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
				new () { Identifer = "TransactionId", ColumnName = "transaction_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", TypeName = "int8" },
				new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "DatasourceId"}},
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
				new () { Identifer = "ProcessId", ColumnName = "process_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "TransactionId", ColumnName = "transaction_id", TypeName = "int8" },
				new () { Identifer = "DatasourceId", ColumnName = "datasource_id", TypeName = "int8" },
				new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "TransactionId"}},
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "DatasourceId"}},
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
				new () { ColumnName = "process_result_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new () { Identifer = "ProcessId", ColumnName = "process_id", TypeName = "int8" },
				new () { Identifer = "FunctionName", ColumnName = "function_name", TypeName = "text" },
				new () { Identifer = "TableName", ColumnName = "table_name", TypeName = "text" },
				new () { Identifer = "Action", ColumnName = "action", TypeName = "text" },
				new () { Identifer = "ResultCount", ColumnName = "result_count", TypeName = "int8" },
				new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "clock_timestamp()" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { ColumnDefinitionNames = new() { "ProcessId"}},
			}
		};
		return t;
	}
}
