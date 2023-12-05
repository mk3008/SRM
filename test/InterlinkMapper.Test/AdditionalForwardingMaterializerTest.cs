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
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly MaterializeServiceProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	//private DbDatasource GetTestDatasouce()
	//{
	//	return DatasourceRepository.sales;
	//}

	//private MaterializeResult GetDummyRequestMeterial()
	//{
	//	return new MaterializeResult()
	//	{
	//		MaterialName = "__additional_request",
	//	};
	//}

	[Fact]
	public void TestCreateRequestMaterialTableQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialTableQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __additional_request
AS
SELECT
    r.sale_journals__ri_sales_id,
    r.sale_id,
    r.created_at,
    ROW_NUMBER() OVER(
        PARTITION BY
            r.sale_id
        ORDER BY
            r.sale_journals__ri_sales_id
    ) AS row_num
FROM
    sale_journals__ri_sales AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateRequestMaterialTableQueryAsReverseCascade()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialTableQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __additional_request
AS
SELECT
    r.sale_journals__ri_sales_id,
    r.sale_id,
    r.created_at,
    ROW_NUMBER() OVER(
        PARTITION BY
            r.sale_id
        ORDER BY
            r.sale_journals__ri_sales_id
    ) AS row_num
FROM
    sale_journals__ri_sales AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(datasource, requestMaterial);

		var expect = """
DELETE FROM
    sale_journals__ri_sales AS d
WHERE
    (d.sale_journals__ri_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__ri_sales_id
        FROM
            sale_journals__ri_sales AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __additional_request AS x
                WHERE
                    x.sale_journals__ri_sales_id = r.sale_journals__ri_sales_id
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
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;
		var query = Proxy.CleanUpMaterialRequestQuery(datasource, requestMaterial);

		var expect = """
DELETE FROM
    __additional_request AS d
WHERE
    (d.sale_id) IN (
        /* exclude requests that exist in the keymap from forwarding */
        SELECT
            r.sale_id
        FROM
            __additional_request AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    sale_journals__m_sales AS x
                WHERE
                    x.sale_id = r.sale_id
            ) OR r.row_num <> 1
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
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

		var query = Proxy.CreateAdditionalDatasourceMaterialQuery(datasource, requestMaterial, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __datasource
AS
WITH
    _target_datasource AS (
        /* inject request material filter */
        SELECT
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.sale_id,
            rm.root__sale_journal_id,
            rm.origin__sale_journal_id
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
            INNER JOIN __additional_request AS rm ON d.sale_id = rm.sale_id
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDatasourceMaterialQuery_Has_raw_CTE()
	{
		var datasource = DatasourceRepository.cte_sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

		var query = Proxy.CreateAdditionalDatasourceMaterialQuery(datasource, requestMaterial, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __datasource
AS
WITH
    __raw AS (
        /* inject request material filter */
        SELECT
            s.sale_date AS journal_closing_date,
            s.sale_date,
            s.shop_id,
            s.price,
            s.sale_id,
            s.sale_detail_id,
            rm.root__sale_journal_id,
            rm.origin__sale_journal_id
        FROM
            sale_detail AS s
            INNER JOIN __additional_request AS rm ON s.sale_id = rm.sale_id
    ),
    _target_datasource AS (
        /* inject request material filter */
        SELECT
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.sale_id,
            rm.root__sale_journal_id,
            rm.origin__sale_journal_id
        FROM
            (
                /* raw data source */
                SELECT
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    SUM(d.price) AS price,
                    d.sale_id
                FROM
                    __raw AS d
                GROUP BY
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.sale_id
            ) AS d
            INNER JOIN __additional_request AS rm ON d.sale_id = rm.sale_id
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
