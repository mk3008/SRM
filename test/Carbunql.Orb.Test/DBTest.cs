using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.DBTestModels;
using Dapper;
using Xunit.Abstractions;

namespace Carbunql.Orb.Test;

public class DBTest
{
	public DBTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger() { Output = output };
	}

	private readonly UnitTestLogger Logger;

	private Destination GetDestination()
	{
		return new Destination()
		{
			DestinationTableName = "public.sale_journals",
			Sequence = new()
			{
				ColumnName = "sale_journal_id",
				CommandText = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
			},
			DbTable = new()
			{
				SchemaName = "public",
				TableName = "sale_journals",
				ColumnNames = { "sale_journal_id", "sale_date", "journal_closing_date", "shop_id", "price" }
			},
			DeleteOption = new()
			{
				RequestTable = new()
				{
					SchemaName = "public",
					TableName = "sale_journal_delete_requests",
					ColumnDefinitions = new()
					{
						new() { Identifer = "RequestId", ColumnName = "sale_journal_delete_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
						new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
						new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
					},
				}
			},
			ValidateOption = new()
			{
				RequestTable = new()
				{
					SchemaName = "public",
					TableName = "sale_journal_validate_requests",
					ColumnDefinitions = new()
					{
						new() { Identifer = "RequestId", ColumnName = "sale_journal_validate_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
						new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
						new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
					},
				}
			},
			FlipOption = new()
			{
				RequestTable = new()
				{
					SchemaName = "public",
					TableName = "sale_journal_flip_requests",
					ColumnDefinitions = new()
					{
						new() { Identifer = "RequestId", ColumnName = "sale_journal_flip_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
						new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
						new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
					},
				}
			},
		};
	}

	private Datasource GetDatasource()
	{
		return new Datasource()
		{
			DatasourceName = "sales",
			Destination = GetDestination(),
			KeyColumnNames = new() { "sale_id" },
			KeymapTable = new()
			{
				TableName = "sale_journals__key_sales",
				ColumnDefinitions = new()
				{
					new () { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
					new () { ColumnName = "sale_id", TypeName = "int8", IsUniqueKey = true },
					new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				},
			},
			RelationmapTable = new()
			{
				TableName = "sale_journals__rel_sales",
				ColumnDefinitions = new()
				{
					new () { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
					new () { Identifer = "SaleId", ColumnName = "sale_id", TypeName = "int8" },
					new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				},
				Indexes = new()
				{
					new () { Identifers = new() { "SaleId" }},
				}
			},
			ForwardRequestTable = new()
			{
				TableName = "sale_journals__fwd_sales",
				ColumnDefinitions = new()
				{
					new () { ColumnName = "request_id", TypeName = "int8" , IsPrimaryKey = true, IsAutoNumber = true },
					new () { ColumnName = "sale_id", TypeName = "int8" , IsUniqueKey= true },
					new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				},
			},
			ValidateRequestTable = new()
			{
				TableName = "sale_journals__vld_sales",
				ColumnDefinitions = new()
				{
					new () { ColumnName = "sale_journal_id", TypeName = "int8", IsPrimaryKey = true },
					new () { Identifer = "SaleId", ColumnName = "sale_id", TypeName = "int8" },
					new () { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp", SpecialColumn = SpecialColumn.CreateTimestamp },
				},
				Indexes = new()
				{
					new () { Identifers = new() { "SaleId" }},
				}
			},
			Query = @"
select
	s.sale_date as journal_closing_date,
	s.sale_date,
	s.shop_id,
	s.price,
	s.sale_id	
from
	sales as s"
		};
	}

	[Fact]
	public void Execute()
	{
		using var cn = (new PostgresDB()).ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		cn.CreateTableOrDefault<Destination>();
		cn.CreateTableOrDefault<Datasource>();

		var destination = GetDestination();
		var datasource = GetDatasource();

		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var sql = @"select 
    ds.datasource_id AS DatasourceId,
    ds.datasource_name AS DatasourceName,
    ds.description AS Description,
    ds.destination_id AS DestinationId,
    ds.query AS Query,
    ds.key_columns_text AS KeyColumnNames,
    ds.keymap_table AS KeymapTable,
    ds.relationmap_table_text AS RelationmapTable,
    ds.forward_request_table_text AS ForwardRequestTable,
    ds.validate_request_table_text AS ValidateRequestTable,
    ds.is_enabled AS IsEnabled,
	--split
    dest.destination_id AS DestinationId,
    dest.destination_table_name AS DestinationTableName,
    dest.description AS Description,
    dest.db_table_text AS DbTable,
    dest.sequence_text AS Sequence,
    dest.validate_option AS ValidateOption,
    dest.flip_option AS FlipOption,
    dest.delete_option AS DeleteOption
from 
	datasources ds 
	inner join destinations dest on ds.destination_id = dest.destination_id";

		var s = cn.Query<Datasource, Destination, Datasource>(sql, (x, y) =>
		{
			x.Destination = y;
			return x;
		}, splitOn: "DestinationId");

		//insert
		ac.Save(cn, destination);

		//update
		destination.FlipOption = null;
		ac.Save(cn, destination);

		//select
		//var dest = ac.FindById<Destination>(cn, destination.DestinationId);

		//var ds = ac.Load<Datasource>(cn, 1);


		//delete
		ac.Delete(cn, destination);

		trn.Commit();
	}
}
