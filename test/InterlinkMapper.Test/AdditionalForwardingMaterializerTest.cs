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

	private DbDatasource GetTestDatasouce()
	{
		return DatasourceRepository.sales;
	}

	private MaterializeResult GetDummyRequestMeterial()
	{
		return new MaterializeResult()
		{
			MaterialName = "__request",
		};
	}

	[Fact]
	public void TestCreateRequestMaterialTableQuery()
	{
		var datasource = GetTestDatasouce();
		var query = Proxy.CreateRequestMaterialTableQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __request
AS
SELECT
    r.sale_journals__r_sales_id,
    r.sale_id,
    r.created_at
FROM
    sale_journals__r_sales AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = GetTestDatasouce();
		var requestMaterial = GetDummyRequestMeterial();
		var query = Proxy.CreateOriginDeleteQuery(requestMaterial, datasource);

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
		var datasource = GetTestDatasouce();
		var requestMaterial = GetDummyRequestMeterial();
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
	public void TestCreateDatasourceMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;

		var requestMaterial = GetDummyRequestMeterial();
		var query = Proxy.CreateDatasourceMaterialQuery(requestMaterial, datasource, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __datasource
AS
WITH
    _target_datasource AS (
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
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
