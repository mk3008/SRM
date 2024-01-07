using Carbunql.Extensions;
using Dapper;
using InterlinkMapper.Models;
using RedOrb;
using RedOrb.Attributes;
using System.Data;
using System.Runtime.CompilerServices;

namespace PostgresSample;

internal static class ApplicationInitializer
{
	[ModuleInitializer]
	public static void Initialize()
	{
		var env = GetEnvironment();
		var cnv = new Converter { Environment = env };

		//RedOrb setting
		ObjectRelationMapper.Converter = cnv.Convert;
		ObjectRelationMapper.PlaceholderIdentifer = env.DbEnvironment.PlaceHolderIdentifer;
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkDestination>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkDatasource>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkTransaction>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkProcess>());

		//Dapper setting
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<Sequence>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<List<KeyColumn>>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<ReverseOption>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<DbTable>());
	}

	public static SystemEnvironment GetEnvironment()
	{
		var env = new SystemEnvironment();
		env.DbTableConfig.ControlTableSchemaName = "interlink";
		return env;
	}

	private class Converter
	{
		public required SystemEnvironment Environment { get; set; }

		public DbTableDefinition Convert(DbTableDefinition def)
		{
			//override schema
			def.SchemaName = Environment.DbTableConfig.ControlTableSchemaName;

			//override type
			foreach (var item in def.ColumnDefinitions)
			{
				if (item.IsAutoNumber)
				{
					item.ColumnType = Environment.DbEnvironment.AutoNumberTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("numeric"))
				{
					item.ColumnType = Environment.DbEnvironment.NumericTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("text"))
				{
					item.ColumnType = Environment.DbEnvironment.TextTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("timestamp"))
				{
					item.ColumnType = Environment.DbEnvironment.TimestampTypeName;
				}
			}
			return def;
		}
	}

	public static void CreateSystemTable(IDbConnection cn)
	{
		if (cn.ExecuteScalar<bool>("""
select exists (
	select * from information_schema.tables where table_schema = 'interlink' and table_name = 'interlink_destination'
)
""")) return;

		var env = GetEnvironment();

		cn.Execute($"create schema if not exists {env.DbTableConfig.ControlTableSchemaName};");

		cn.CreateTableOrDefault<InterlinkDestination>();
		cn.CreateTableOrDefault<InterlinkDatasource>();

		cn.CreateTableOrDefault<InterlinkTransaction>();
		cn.CreateTableOrDefault<InterlinkProcess>();
	}

	public static void CreateApplicationMasterData(IDbConnection cn)
	{
		if (cn.ExecuteScalar<bool>("""
select case when count(*) = 0 then false else true end from interlink.interlink_destination
""")) return;

		var env = GetEnvironment();

		var destinations = DestinationRepository.GetAll();
		foreach (var item in destinations)
		{
			cn.Save(item);
		}

		foreach (var destination in destinations)
		{
			cn.CreateTableOrDefault(destination.GetInterlinkRelationTable(env).Definition);
			cn.CreateTableOrDefault(destination.GetReverseRequestTable(env).Definition);

			foreach (var source in destination.Datasources)
			{
				cn.CreateTableOrDefault(source.GetKeyMapTable(env).Definition);
				cn.CreateTableOrDefault(source.GetKeyRelationTable(env).Definition);

				cn.CreateTableOrDefault(source.GetInsertRequestTable(env).Definition);
				cn.CreateTableOrDefault(source.GetValidationRequestTable(env).Definition);
			}
		}
	}

	public static void CreateApplicationTable(IDbConnection cn)
	{
		if (cn.ExecuteScalar<bool>("""
select exists (
	select * from information_schema.tables where table_schema = 'public' and table_name = 'sale_journal'
)
""")) return;

		cn.Execute("""
create table if not exists sale_journal (
	sale_journal_id serial8 not null, 
	sale_date date not null, 
	price int8 not null, 
	created_at timestamp not null default current_timestamp, 
	primary key(sale_journal_id)
)
;
create table if not exists sale (
    sale_id serial8 not null, 
    sale_date date not null, 
	price int8 not null,
    created_at timestamp not null default current_timestamp, 
    primary key(sale_id)
)
;
--Table to manage all customers
create table if not exists customer (
	customer_id serial8 not null, 
	customer_type int4 not null, 
	customer_name text not null, 
	created_at timestamp not null default current_timestamp, 
	primary key(customer_id)
)
;
--Corporate customers
create table if not exists corporate_customer (
    corporate_customer_id serial8 not null, 
    company_name text not null, 
    created_at timestamp not null default current_timestamp, 
    primary key(corporate_customer_id)
)
;
--online shop customers
create table if not exists net_member (
    net_member_id serial8 not null, 
    user_name text not null, 
    created_at timestamp not null default current_timestamp, 
    primary key(net_member_id)
)
""");
	}

	public static void CreateApplicationTransactionData(IDbConnection cn)
	{
		if (cn.ExecuteScalar<bool>("""
select case when count(*) = 0 then false else true end from sale
""")) return;

		cn.Execute("""
INSERT INTO sale (sale_date, price) VALUES
('2023-01-01', 100),
('2023-02-05', 150),
('2023-03-12', 200),
('2023-04-18', 120),
('2023-05-24', 180),
('2023-06-30', 220),
('2023-07-07', 130),
('2023-08-15', 190),
('2023-09-22', 250),
('2023-10-28', 140)
;
insert into corporate_customer (company_name)
select
    'company ' || generate_series(1, 10)
;
insert into net_member (user_name)
select
    'user ' || generate_series(1, 10)
""");
	}
}