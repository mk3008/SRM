using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class AdditionalForwardingMaterializerTest
{
	public AdditionalForwardingMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new AdditionalForwardingMaterializer(Environment).AsPrivateProxy();
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly MaterializeServiceProxy Proxy;

	private MaterializeResult CreateDatasourceMeterial()
	{
		var datasource = DatasourceRepository.sales;
		var sq = new SelectQuery(datasource.Query);
		return Proxy.CreateResult("__datasource", 10, sq);
	}

	private MaterializeResult CreateRequestMeterial()
	{
		var datasource = DatasourceRepository.sales;
		var request = Environment.GetInsertRequestTable(datasource);
		return Proxy.CreateResult("__request", 10, request.ToSelectQuery());
	}

	[Fact]
	public void TestCreateResult_Request()
	{
		var request = CreateRequestMeterial();

		var expect = """
/* select material table */
SELECT
    d.sale_journals__r_sales_id,
    d.sale_id,
    d.created_at
FROM
    __request AS d
""";
		var actual = request.SelectQuery.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(10, request.Count);
		Assert.Equal("__request", request.MaterialName);
		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateResult_Datasource()
	{
		var result = CreateDatasourceMeterial();

		var expect = """
/* select material table */
SELECT
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id
FROM
    __datasource AS d
""";
		var actual = result.SelectQuery.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(10, result.Count);
		Assert.Equal("__datasource", result.MaterialName);
		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	//[Fact]
	//public void TestCreateMaterialSelelectQuery()
	//{
	//	var datasource = DatasourceRepository.sales;
	//	Proxy.CreateMaterialSelelectQuery("material", )
	//}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;

		var request = CreateRequestMeterial();
		var query = Proxy.CreateOriginDeleteQuery(request, datasource);

		var expect = """
DELETE FROM
    sale_journals__r_sales AS d
WHERE
    (d.sale_journals__r_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__r_sales_id
        FROM
            sale_journals__r_sales AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __request AS x
                WHERE
                    x.sale_journals__r_sales_id = r.sale_journals__r_sales_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCleanUpMaterialRequestQuery()
	{
		var datasource = DatasourceRepository.sales;

		var requestMaterial = CreateRequestMeterial();
		var query = Proxy.CleanUpMaterialRequestQuery(requestMaterial, datasource);

		var expect = """
DELETE FROM
    __request AS d
WHERE
    (d.sale_id) IN (
        /* exclude requests that exist in the keymap from forwarding */
        SELECT
            r.sale_id
        FROM
            __request AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    sale_journals__m_sales AS x
                WHERE
                    x.sale_id = r.sale_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDatasourceSelectQuery()
	{
		var datasource = DatasourceRepository.sales;

		var request = CreateRequestMeterial();
		var query = Proxy.CreateDatasourceSelectQuery(request, datasource, (SelectQuery x) => x);

		var expect = """
/* data source to be added */
SELECT
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id
FROM
    (
        /* raw data source */
        SELECT
            s.sale_date AS journal_closing_date,
            s.sale_date,
            s.shop_id,
            s.price,
            s.sale_id
        FROM
            sales AS s
    ) AS d
WHERE
    EXISTS (
        /* exists request material */
        SELECT
            *
        FROM
            __request AS x
        WHERE
            x.sale_id = d.sale_id
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
