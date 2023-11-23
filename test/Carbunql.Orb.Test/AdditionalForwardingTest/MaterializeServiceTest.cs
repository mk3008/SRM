using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using PostgresSample;
using Xunit.Abstractions;

namespace AdditionalForwardingTest;

public class MaterializeServiceTest //: IClassFixture<PostgresDB>
{
	public MaterializeServiceTest(ITestOutputHelper output)//(PostgresDB postgresDB, ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	[Fact]
	public void TestCreateResult()
	{
		var sq = new SelectQuery("select a.id, 'data' as text, 1 as value from table_a as a");

		var service = new MaterializeService(Environment);
		var result = service.AsPrivateProxy().CreateResult("test", 10, sq);

		var expect = """
/* select material table */
SELECT
	d.id,
    d.text,
    d.value
FROM
	test AS d
""";
		var actual = result.SelectQuery.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(10, result.Count);
		Assert.Equal("test", result.MaterialName);
		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
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
