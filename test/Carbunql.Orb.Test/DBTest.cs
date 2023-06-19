﻿using Carbunql.Orb.Test.Models;
using Carbunql.Orb.Test.TypeHandlers;
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

	[Fact]
	public void Execute()
	{
		var def = DefinitionRepository.GetDestinationTableDefinition();

		using var cn = (new PostgresDB()).ConnectionOpenAsNew();
		cn.Execute(def.ToCreateTableCommandText());
		foreach (var item in def.ToCreateIndexCommandTexts()) cn.Execute(item);

		var SaleJournalDestination = new Destination()
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
					//Type = typeof(DeleteOption),
					ColumnDefinitions = new() {
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
					//Type = typeof(ValidateOption),
					ColumnDefinitions = new() {
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
					//Type = typeof(FlipOption),
					ColumnDefinitions = new() {
						new() { Identifer = "RequestId", ColumnName = "sale_journal_flip_request_id", TypeName = "serial8" , IsPrimaryKey = true, IsAutoNumber = true },
						new() { ColumnName = "sale_journal_id", TypeName = "int8" , IsUniqueKey= true },
						new() { ColumnName = "created_at", TypeName = "timestamp", DefaultValue = "current_timestamp" },
					},
				}
			},
		};

		SqlMapper.AddTypeHandler(new DbTableTypeHandler());
		SqlMapper.AddTypeHandler(new SequenceTypeHandler());
		SqlMapper.AddTypeHandler(new DbTableDefinitionTypeHandler());
		SqlMapper.AddTypeHandler(new ListStringTypeHandler());
		SqlMapper.AddTypeHandler(new ValidateOptionTypeHandler());
		SqlMapper.AddTypeHandler(new FlipOptionTypeHandler());
		SqlMapper.AddTypeHandler(new DeleteOptionTypeHandler());

		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };
		ac.Save(cn, def, SaleJournalDestination);

		//var loaddef = cn.Load<Destination>(def, 1);

		//ac.Delete(cn, def, SaleJournalDestination);
	}
}
