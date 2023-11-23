using Carbunql.Dapper;
using Dapper;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using PostgresSample;
using Xunit.Abstractions;

namespace AdditionalForwardingTest;

public class UnitTest1 : IClassFixture<PostgresDB>
{
	public UnitTest1(PostgresDB postgresDB, ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);
		PostgresDB = postgresDB;
		PostgresDB.Logger = Logger;

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = postgresDB,
		};

		using var cn = PostgresDB.ConnectionOpenAsNew();
		var sql = """
create table sales (
    sale_id serial8 not null primary key,
    sale_date date not null,
    shop_id int8 not null,
    price int8,
    created_at timestamp default current_timestamp,     
    updated_at timestamp default current_timestamp     
)
;
create table sale_journals (
    sale_journal_id serial8 not null primary key,
    journal_closing_date date not null,
    sale_date date not null,
    shop_id int8 not null,
    price int8,
    created_at timestamp default current_timestamp
)
;
create table journal_closings (
    journal_closing_id serial8 not null primary key,
    journal_closing_date date not null,
    created_at timestamp default current_timestamp
)
;
create table shop_journal_closings (
    shop_journal_closing_id serial8 not null primary key,
    shop_id int8 not null,
    journal_closing_date date not null,
    created_at timestamp default current_timestamp
)
;
CREATE UNIQUE INDEX i1_shop_journal_closings ON shop_journal_closings (shop_id, journal_closing_date);
;
--test data
INSERT INTO sales (sale_date, shop_id, price, created_at)
select
    sale_date,
    shop_id,
    price,
    sale_date::timestamp + '-1 day' + random() * interval '3 days' as created_at
from
    (
        select
            date_trunc('day', CURRENT_DATE::timestamp + '1 day' - random() * interval '2 month')::date AS sale_date,
            floor(random() * 3) + 1 AS shop_id,
            floor(random() * 10000) + 1 AS price
        --    CURRENT_DATE::timestamp + '1 day' - random() * interval '2 month' as created_at
        FROM
            generate_series(1, 10000)
    ) d
;
insert into journal_closings (
    journal_closing_date,
    created_at
)
select
    journal_closing_date,
    (sale_date::timestamp + interval '19 hours')::timestamp + random() * interval '6 hours' as created_at
from
    (
        select distinct
            sale_date as journal_closing_date,
            sale_date
        from
            sales
    ) d
order by 
    journal_closing_date
;
insert into shop_journal_closings (
    journal_closing_date,
    shop_id,
    created_at
)
select
    journal_closing_date,
    shop_id,
    (sale_date::timestamp + interval '17 hours')::timestamp + random() * interval '1 day' as created_at
from
    (
        select distinct
            sale_date as journal_closing_date,
            sale_date,
            shop_id        
        from
            sales
    ) d
order by 
    sale_date,
    shop_id
""";
		//create table and insert data
		cn.Execute(sql);
	}

	private readonly PostgresDB PostgresDB;

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	[Fact]
	public void InitTest_shop_journal_closings()
	{
		var cn = PostgresDB.ConnectionOpenAsNew();
		var rows = cn.Query("select * from shop_journal_closings");
		OutputRows(rows);
	}

	[Fact]
	public void InitTest_journal_closings()
	{
		var cn = PostgresDB.ConnectionOpenAsNew();
		var rows = cn.Query("select * from journal_closings");
		OutputRows(rows);
	}

	[Fact]
	public void InitTest_sales()
	{
		var cn = PostgresDB.ConnectionOpenAsNew();
		var rows = cn.Query("select * from sales");
		OutputRows(rows);
	}

	private void OutputRows(dynamic rows)
	{
		var isFirst = true;
		foreach (var row in rows)
		{
			if (isFirst)
			{
				isFirst = false;
				var head = string.Empty;
				foreach (var property in (IDictionary<string, object>)row)
				{
					if (head != string.Empty) head += ", ";
					head += property.Key;
				}
				Logger.LogInformation(head);
			}
			var text = string.Empty;
			foreach (var property in (IDictionary<string, object>)row)
			{
				if (text != string.Empty) text += ", ";
				text += property.Value.ToString();
			}
			Logger.LogInformation(text);
		}
	}
}

internal static class DestinationConfig
{
	public static DbDestination sale_journals => new DbDestination()
	{
		DestinationId = 1,
		Table = new()
		{
			TableName = "sale_journals",
			Columns = new() {
					"sale_journal_id",
					"journal_closing_date",
					"sale_date",
					"shop_id",
					"price",
					"remarks"
				}
		},
		Sequence = new()
		{
			Column = "sale_journal_id",
			Command = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
		},
		ReversalOption = null
	};

}

internal static class DatasourceConfig
{
	public static DbDatasource sales =>
		new DbDatasource()
		{
			DatasourceId = 1,
			DatasourceName = "sales",
			Destination = DestinationConfig.sale_journals,
			KeyColumns = new() {
				new () {
					ColumnName = "sale_id",
					TypeName = "int8"
				}
			},
			KeyName = "sales",
			Query = @"
select
	s.sale_date as journal_closing_date,
	s.sale_date,
	s.shop_id,
	s.price,
	--key
	s.sale_id	
from
	sales as s
",
		};
}
