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

	[Fact]
	public void TestCreateRequestMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __additional_request
AS
SELECT
    r.sale_journals__ri_sales_id,
    r.sale_id,
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
                    sale_journals__km_sales AS x
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
	public void TestCreateMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

		var query = Proxy.CreateAdditionalMaterialQuery(datasource, requestMaterial, (SelectQuery x) => x);

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
            INNER JOIN __additional_request AS rm ON d.sale_id = rm.sale_id
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

	[Fact]
	public void TestCreateKeyRelationInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = material.AsPrivateProxy().CreateKeyRelationInsertQuery();
		var expect = """
INSERT INTO
    sale_journals__kr_sales (
        sale_journal_id, sale_id
    )
SELECT
    d.sale_journal_id,
    d.sale_id
FROM
    (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateKeyMapInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = material.AsPrivateProxy().CreateKeyMapInsertQuery();
		var expect = """
INSERT INTO
    sale_journals__km_sales (
        sale_journal_id, sale_id
    )
SELECT
    d.sale_journal_id,
    d.sale_id
FROM
    (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDestinationInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = ((MaterializeResult)material).AsPrivateProxy().CreateDestinationInsertQuery();
		var expect = """
INSERT INTO
    sale_journals (
        sale_journal_id, journal_closing_date, sale_date, shop_id, price
    )
SELECT
    d.sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price
FROM
    (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateRelationInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = ((MaterializeResult)material).AsPrivateProxy().CreateRelationInsertQuery(1);
		var expect = """
/*
  :interlink__process_id = 1
*/
INSERT INTO
    sale_journals__relation (
        interlink__process_id, sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink__remarks
    )
WITH
    d AS (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __datasource AS t
    )
SELECT
    :interlink__process_id AS interlink__process_id,
    d.sale_journal_id,
    COALESCE(kr.root__sale_journal_id, d.sale_journal_id) AS root__sale_journal_id,
    d.sale_journal_id AS origin__sale_journal_id,
    null AS interlink__remarks
FROM
    d
    LEFT JOIN (
        /* if reverse transfer is performed, one or more rows exist. */
        SELECT
            kr.sale_id,
            kr.sale_journal_id AS root__sale_journal_id
        FROM
            (
                SELECT
                    kr.sale_id,
                    kr.sale_journal_id,
                    ROW_NUMBER() OVER(
                        PARTITION BY
                            kr.sale_id
                        ORDER BY
                            kr.sale_journal_id
                    ) AS _row_num
                FROM
                    sale_journals__kr_sales AS kr
                    INNER JOIN d ON kr.sale_id = d.sale_id
            ) AS kr
        WHERE
            kr._row_num = 1
    ) AS kr ON d.sale_id = kr.sale_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
