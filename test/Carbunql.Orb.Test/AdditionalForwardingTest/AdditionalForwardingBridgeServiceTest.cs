using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using PostgresSample;
using Xunit.Abstractions;

namespace AdditionalForwardingTest;

public class AdditionalForwardingBridgeServiceTest
{
	public AdditionalForwardingBridgeServiceTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new AdditionalForwardingBridgeService(Environment).AsPrivateProxy();
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly AdditionalForwardingBridgeServiceProxy Proxy;

	private MaterializeResult CreateRequestResult(DbDatasource d)
	{
		var sq = new SelectQuery(d.Query);
		var proxy = new MaterializeService(Environment).AsPrivateProxy();
		return proxy.CreateResult("material", 10, sq);
	}

	[Fact]
	public void TestGenerateMaterialName()
	{
		var expect = "__m_4e019cf3";
		var actual = Proxy.GenerateMaterialName("table_a");
		Logger.LogInformation(actual);

		Assert.Equal(expect, actual);
	}

	[Fact]
	public void TestCreateDatasourceSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = CreateRequestResult(datasource);

		var expect = """
/* Data source to be added */
SELECT
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id
FROM
    (
        SELECT
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.sale_id
        FROM
            (
                SELECT
                    s.sale_date AS journal_closing_date,
                    s.sale_date,
                    s.shop_id,
                    s.price,
                    s.sale_id
                FROM
                    sales AS s
            ) AS d
    ) AS d
WHERE
    EXISTS (
        /* exists request */
        SELECT
            *
        FROM
            material AS x
        WHERE
            x.sale_id = d.sale_id
    )
    AND NOT EXISTS (
        /* not exists keymap */
        SELECT
            *
        FROM
            sale_journals__m_sales AS x
        WHERE
            x.sale_id = d.sale_id
    )
""";
		var injector = (SelectQuery x) => x;
		var actual = Proxy.CreateDatasourceSelectQuery(datasource, request, injector).ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
