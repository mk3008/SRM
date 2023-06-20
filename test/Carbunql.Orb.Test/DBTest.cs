using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.Models;
using Dapper;
using Xunit.Abstractions;

namespace Carbunql.Orb.Test;

public class DBTest
{
	public DBTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger() { Output = output };

		//Dapper
		SqlMapper.AddTypeHandler(new DbTableTypeHandler());
		SqlMapper.AddTypeHandler(new SequenceTypeHandler());
		SqlMapper.AddTypeHandler(new DbTableDefinitionTypeHandler());
		SqlMapper.AddTypeHandler(new ListStringTypeHandler());
		SqlMapper.AddTypeHandler(new ValidateOptionTypeHandler());
		SqlMapper.AddTypeHandler(new FlipOptionTypeHandler());
		SqlMapper.AddTypeHandler(new DeleteOptionTypeHandler());

		//Carbunql.Orb
		var destdef = DefinitionRepository.GetDestinationTableDefinition();
		var sourcedef = DefinitionRepository.GetDatasourceTableDefinition();
		ObjectTableMapper.Add(destdef);
		ObjectTableMapper.Add(sourcedef);
	}

	private readonly UnitTestLogger Logger;

	private Destination GetDestination()
	{
		return new Destination()
		{
			DestinationTableName = "public.sale_journals",
			Sequence = new()
			{
				Column = "sale_journal_id",
				Command = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
			},
			DbTable = new()
			{
				SchemaName = "public",
				TableName = "sale_journals",
				Columns = { "sale_journal_id", "sale_date", "journal_closing_date", "shop_id", "price" }
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
					new DbIndexDefinition() { ColumnDefinitionNames = new() { "SaleId" }},
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
					new DbIndexDefinition() { ColumnDefinitionNames = new() { "SaleId" }},
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
		//var destdef = DefinitionRepository.GetDestinationTableDefinition();
		//var sourcedef = DefinitionRepository.GetDatasourceTableDefinition();

		////Carbunql.Orb
		//ObjectTableMapper.Add<Destination>(destdef);

		using var cn = (new PostgresDB()).ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		cn.CreateTableOrDefault<Destination>();// (destdef);
		cn.CreateTableOrDefault<Datasource>();// (sourcedef);

		var destination = GetDestination();
		var datasource = GetDatasource();

		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		//insert
		ac.Save(cn, destination);

		//update
		destination.FlipOption = null;
		ac.Save(cn, destination);

		//select
		var dest = ac.Load<Destination>(cn, destination.DestinationId);

		//delete
		ac.Delete(cn, destination);

		trn.Commit();
	}
}
