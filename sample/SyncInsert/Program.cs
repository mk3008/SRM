using Carbunql.Dapper;
using Dapper;
using InterlinkMapper;
using InterlinkMapper.Actions;
using InterlinkMapper.Batches;
using SyncInsert;

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
			new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8", IsUniqueKey = true }
		},
		},
		RelationMapTable = new()
		{
			TableName = "sale_journals__rel_sales",
			ColumnDefinitions = new() {
			new ColumnDefinition() { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
			new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8" }
		},
		},
		RequestTable = new()
		{
			TableName = "sale_journals__req_sales",
			ColumnDefinitions = new() {
			new ColumnDefinition() { ColumnName = "request_id", TypeName = "int8" , IsPrimaryKey = true, IsAutoNumber = true },
			new ColumnDefinition() { ColumnName = "sale_id", TypeName = "int8" , IsUniqueKey= true }
		},
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
var datasource = GetDatasource();

var batch = new ForwardTransferBatch(connector, logger);
batch.Execute(datasource);

ExecuteSql(connector, "update sales set remarks = 'remarks_0' where remarks = 'remarks_1'");

batch.Execute(datasource);

Console.ReadLine();
