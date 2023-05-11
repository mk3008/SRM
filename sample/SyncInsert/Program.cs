using Carbunql.Dapper;
using Dapper;
using InterlinkMapper;
using InterlinkMapper.Actions;
using InterlinkMapper.Batches;
using SyncInsert;

DbEnvironment GetDbEnvironment()
{
	return new DbEnvironment()
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
		ProcessnTable = new()
		{
			TableName = "processes",
			ColumnDefinitions = new()
			{
				new ColumnDefinition() { ColumnName = "process_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "transaction_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "destination_table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "datasource_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "keymap_table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "relationmap_table_name", TypeName = "text" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "transaction_id" }},
				new DbIndexDefinition() { IndexNumber = 2, Columns = new() { "destination_table_name", "datasource_name" }},
				new DbIndexDefinition() { IndexNumber = 3, Columns = new() { "keymap_table_name" }}
			}
		},
		TransactionIdColumn = "transaction_id",
		ProcessIdColumn = "process_id",
		DestinationTableNameColumn = "destination_table_name",
		DatasourceNameColumn = "datasource_name",
		KeymapTableNameColumn = "keymap_table_name",
		RelationmapTableNameColumn = "relationmap_table_name",
		TimestampColumn = "created_at",
		PlaceHolderIdentifer = ":"
	};
}

DbDestination GetDestination()
{
	return new DbDestination()
	{
		Table = new()
		{
			TableName = "sale_journals",
			Columns = new() { "sale_journal_id", "sale_date", "price", "remarks" }
		},
		Sequence = new()
		{
			Column = "sale_journal_id",
			Command = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
		},
		ProcessTable = new()
		{
			TableName = "sale_journal_processes",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "process_id", TypeName = "int8" },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
			Indexes = new()
			{
				new DbIndexDefinition() { IndexNumber = 1, Columns = new() { "process_id" }},
			}
		},
		ValidateRequestTable = new()
		{
			TableName = "sale_journal_validate_requests",
			ColumnDefinitions = new() {
				new ColumnDefinition() { ColumnName = "sale_journal_validate_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
				new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
				new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
			},
		},
		FlipOption = new()
		{
			FlipTable = new()
			{
				TableName = "sale_journal_flips",
				ColumnDefinitions = new() {
					new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
					new ColumnDefinition() { ColumnName = "flip_sale_journal_id", TypeName = "int8", IsUniqueKey = true },
					new ColumnDefinition() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
				}
			},
			ReversalColumns = new() { "price" },
			ExcludedColumns = new() { "remarks" },
			FlipIdColumn = "flip_sale_journal_id"
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
	s.sale_date,
	s.price,
	s.remarks,
	case when s.remarks = 'remarks_0' then false else true end as _hold,
	--key
	s.sale_id	
from
	sales as s
",
		HoldJudgementColumnName = "_hold"
	};
}

void ExecuteSql(IDbConnectAction connector, string sql)
{
	using var cn = connector.Execute();
	using var trn = cn.BeginTransaction();
	cn.Execute(sql);
	trn.Commit();
}

var connector = new PostgresDbConnectAction();
var logger = new ConsoleLogger();
var environment = GetDbEnvironment();
var datasource = GetDatasource();

var builder = new DbEnvironmentBuildBatch(connector, logger);
builder.Execute(environment, datasource);

var transfer = new ForwardTransferBatch(environment, connector, logger);
transfer.Execute(datasource);

ExecuteSql(connector, "update sales set remarks = 'remarks_0' where remarks = 'remarks_1'");

transfer.Execute(datasource);

Console.ReadLine();
