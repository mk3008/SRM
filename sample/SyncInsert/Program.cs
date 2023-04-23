﻿using InterlinkMapper.Data;
using Npgsql;
using SyncInsert;

var dest = new DbDestination()
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
var ds = new DbDatasource()
{
	Destination = dest,
	KeyColumns = new() { "sale_id" },
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
	Query = @"
select
	s.sale_date,
	s.price,
	s.remarks,
	--key
	s.sale_id	
from
	sales as s
"
};


var cnstring = "Server=localhost;Port=5430;Database=postgres;User Id=root;Password=root;";

using var cn = new NpgsqlConnection(cnstring);
cn.Open();
using var trn = cn.BeginTransaction();

var logger = new ConsoleLogger();

var batch = new ForwardTransferBatch(cn, logger);

//be transferred
batch.Execute(ds);

Console.ReadLine();
