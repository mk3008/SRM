using Carbunql.Dapper;
using Dapper;
using InterlinkMapper;
using InterlinkMapper.Batches;
using InterlinkMapper.System;
using SyncInsert;

DbTableConfig GetDbTableConfig()
{
	return new DbTableConfig()
	{
		TransactionTable = new()
		{
			TableName = "transactions",
			ColumnDefinitions = new()
			{
				new ColumnDefinition() { ColumnName = "transaction_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "destination_table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "datasource_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() {"destination_table_name" , "datasource_name"}}
			}
		},
		ProcessTable = new()
		{
			TableName = "processes",
			ColumnDefinitions = new()
			{
				new ColumnDefinition() { ColumnName = "process_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "transaction_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "destination_table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "datasource_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "keymap_table_name", TypeName = "text", AllowNull = true },
				new ColumnDefinition() { ColumnName = "relationmap_table_name", TypeName = "text", AllowNull = true },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "transaction_id" }},
				new DbIndexDefinition() { IndexNumber = 2, Columns = new() { "destination_table_name", "datasource_name" }},
				new DbIndexDefinition() { IndexNumber = 3, Columns = new() { "keymap_table_name" }}
			}
		},
		ProcessResultTable = new()
		{
			TableName = "process_results",
			ColumnDefinitions = new()
			{
				new ColumnDefinition() { ColumnName = "process_result_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "process_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "function_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "action", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "result_count", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "clock_timestamp()" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "process_id" }},
			}
		},
		TransactionIdColumn = "transaction_id",
		ProcessIdColumn = "process_id",
		DestinationTableNameColumn = "destination_table_name",
		DatasourceNameColumn = "datasource_name",
		KeymapTableNameColumn = "keymap_table_name",
		RelationmapTableNameColumn = "relationmap_table_name",
		MemberNameColumn = "function_name",
		TableNameColumn = "table_name",
		ActionNameColumn = "action",
		ResultCountColumn = "result_count",
		TimestampColumn = "created_at",
	};
}

DbDestination GetDestination()
{
	return new DbDestination()
	{
		Table = new()
		{
			TableName = "sale_journals",
			Columns = new() { "sale_journal_id", "journal_closing_date", "sale_date", "shop_id", "price", "remarks" }
		},
		Sequence = new()
		{
			Column = "sale_journal_id",
			Command = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
		},
		ProcessTable = new()
		{
			Definition = new()
			{
				TableName = "sale_journal_processes",
				ColumnDefinitions = new() {
					new() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
					new() { ColumnName = "root_sale_journal_id", TypeName = "int8" },
					new() { ColumnName = "source_sale_journal_id", TypeName = "int8", AllowNull = true },
					new() { ColumnName = "is_flip", TypeName = "boolean", DefaultValue = "false" },
					new() { ColumnName = "process_id", TypeName = "int8" },
					new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
				},
				Indexes = new()
				{
					new() { IndexNumber = 1, Columns = new() { "process_id" } },
				}
			},
			RootIdColumnName = "root_sale_journal_id",
			SourceIdColumnName = "source_sale_journal_id",
			FlipColumnName = "is_flip",
		},
		ValidateRequestTable = new()
		{
			TableName = "sale_journal_watch_requests",
			ColumnDefinitions = new() {
				new() { ColumnName = "sale_journal_watch_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
				new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
		},
		DeleteRequestTable = new()
		{
			TableName = "sale_journal_delete_requests",
			ColumnDefinitions = new() {
				new() { ColumnName = "sale_journal_delete_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
				new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
		},
		FlipOption = new()
		{
			FlipTable = new()
			{
				TableName = "sale_journal_flip_requests",
				ColumnDefinitions = new() {
					new() { ColumnName = "sale_journal_flip_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
					new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
					new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
				}
			},
			ReversalColumns = new() { "price" },
			ExcludedColumns = new() { "remarks" }
		},
		DbCommonTableExtensions = new()
		{
			new()
			{
				CommanTableExtensionName = "cte_jc",
				Query = "select max(journal_closing_year_month)::timestamp + '1 month' as journal_date from journal_closings",
				ValueColumn = "journal_closing_date",
				DbFunction = "greatest",
			},
			new()
			{
				CommanTableExtensionName = "cte_shop_jc",
				Query = "select shop_id, max(journal_closing_year_month)::timestamp + '1 month' as journal_date from shop_journal_closings group by shop_id",
				ValueColumn = "journal_closing_date",
				DbFunction = "greatest",
				//keycolumns = new() {"shop_id"}
			}
		}
	};
}

DbDatasource GetDatasource()
{
	return new DbDatasource()
	{
		DatasourceName = "sales",
		Destination = GetDestination(),
		KeyColumns = new() { "sale_id" },
		IsSupportSequenceTransfer = true,
		KeyMapTable = new()
		{
			TableName = "sale_journals__key_sales",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
				new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8", IsUniqueKey = true },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
		},
		RelationMapTable = new()
		{
			TableName = "sale_journals__rel_sales",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
				new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "sale_id" }},
			}
		},
		ForwardRequestTable = new()
		{
			TableName = "sale_journals__fwd_sales",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "request_id", TypeName = "int8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8" , IsUniqueKey= true },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
		},
		ValidateRequestTable = new()
		{
			TableName = "sale_journals__vld_sales",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
				new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "sale_id" }},
			}
		},
		Query = @"
select
	s.sale_date as journal_closing_date,
	s.sale_date,
	s.shop_id,
	s.price,
	case when s.price > 10000 then false else true end as _is_hold,
	--key
	s.sale_id	
from
	sales as s
",
		HoldJudgementColumnName = "_is_hold"
	};
}

void ExecuteSql(IDbConnetionConfig connector, string sql)
{
	using var cn = connector.ConnectionOpenAsNew();
	using var trn = cn.BeginTransaction();
	cn.Execute(sql);
	trn.Commit();
}

var dbConnectionConfig = new PostgresDB();
var dbTableConfig = GetDbTableConfig();
var environment = new SystemEnvironment()
{
	DbConnetionConfig = dbConnectionConfig,
	DbTableConfig = dbTableConfig,
	DbQueryConfig = new() { PlaceHolderIdentifer = ":" }
};
var logger = new ConsoleLogger();

var datasource = GetDatasource();

var builder = new DbEnvironmentBuildBatch(environment, logger);
builder.Execute(datasource);

var forwardTransfer = new ForwardTransferBatch(environment, logger);
forwardTransfer.Execute(datasource);

ExecuteSql(dbConnectionConfig, "insert into sale_journals_delete_requests(sale_journal_id) select sale_journal_id from sale_journals");

var deleteTransfer = new DeleteTransferFromRequestBatch(environment, logger);
deleteTransfer.Execute(datasource.Destination);

Console.ReadLine();
