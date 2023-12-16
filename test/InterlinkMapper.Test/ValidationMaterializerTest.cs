using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationMaterializerTest
{
	public ValidationMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ValidationMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly ValidationMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void TestCreateRequestMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_request
AS
SELECT
    r.sale_journals__rv_sales_id,
    m.sale_journal_id,
    m.sale_id
FROM
    sale_journals__rv_sales AS r
    INNER JOIN sale_journals__km_sales AS m ON r.sale_id = m.sale_id
WHERE
    m.sale_journal_id IS NOT null
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(datasource, request);

		var expect = """
DELETE FROM
    sale_journals__rv_sales AS d
WHERE
    (d.sale_journals__rv_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__rv_sales_id
        FROM
            sale_journals__rv_sales AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_journals__rv_sales_id = r.sale_journals__rv_sales_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateExpectValueSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateExpectValueSelectQuery(datasource, request);

		var expect = """
/* inject request material filter */
SELECT
    d.sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.remarks
FROM
    (
        /* destination */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks
        FROM
            sale_journals AS d
    ) AS d
    INNER JOIN __validation_request AS rm ON d.sale_journal_id = rm.sale_journal_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateActualValueSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateActualValueSelectQuery(datasource, request);

		var expect = """
/* inject request material filter */
/* does not exist if physically deleted */
SELECT
    rm.sale_journal_id,
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
    INNER JOIN __validation_request AS rm ON d.sale_id = rm.sale_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateValidationDatasourceSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationDatasourceSelectQuery(datasource, request);

		var expect = """
/* reverse only */
WITH
    _expect AS (
        /* inject request material filter */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks
        FROM
            (
                /* destination */
                SELECT
                    d.sale_journal_id,
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.price,
                    d.remarks
                FROM
                    sale_journals AS d
            ) AS d
            INNER JOIN __validation_request AS rm ON d.sale_journal_id = rm.sale_journal_id
    ),
    _actual AS (
        /* inject request material filter */
        /* does not exist if physically deleted */
        SELECT
            rm.sale_journal_id,
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
            INNER JOIN __validation_request AS rm ON d.sale_id = rm.sale_id
    )
SELECT
    e.sale_journal_id,
    a.sale_id,
    '{"deleted":true}' AS interlink__remarks
FROM
    _expect AS e
    LEFT JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
WHERE
    a.sale_id IS null
UNION ALL
/* reverse and additional */
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"updated":[', SUBSTRING(d.interlink__remarks, 1, LENGTH(d.interlink__remarks) - 1), ']}') AS interlink__remarks
FROM
    (
        SELECT
            e.sale_journal_id,
            a.sale_id,
            CONCAT(CASE
                WHEN e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
            END, CASE
                WHEN e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
            END, CASE
                WHEN e.sale_date IS NOT DISTINCT FROM a.sale_date THEN '"sale_date",'
            END, CASE
                WHEN e.shop_id IS NOT DISTINCT FROM a.shop_id THEN '"shop_id",'
            END, CASE
                WHEN e.price IS NOT DISTINCT FROM a.price THEN '"price",'
            END) AS interlink__remarks
        FROM
            _expect AS e
            INNER JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationMaterialQuery(datasource, request, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    _expect AS (
        /* inject request material filter */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks
        FROM
            (
                /* destination */
                SELECT
                    d.sale_journal_id,
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.price,
                    d.remarks
                FROM
                    sale_journals AS d
            ) AS d
            INNER JOIN __validation_request AS rm ON d.sale_journal_id = rm.sale_journal_id
    ),
    _actual AS (
        /* inject request material filter */
        /* does not exist if physically deleted */
        SELECT
            rm.sale_journal_id,
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
            INNER JOIN __validation_request AS rm ON d.sale_id = rm.sale_id
    ),
    _target_datasource AS (
        /* reverse only */
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS interlink__remarks
        FROM
            _expect AS e
            LEFT JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            a.sale_id IS null
        UNION ALL
        /* reverse and additional */
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"updated":[', SUBSTRING(d.interlink__remarks, 1, LENGTH(d.interlink__remarks) - 1), ']}') AS interlink__remarks
        FROM
            (
                SELECT
                    e.sale_journal_id,
                    a.sale_id,
                    CONCAT(CASE
                        WHEN e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
                    END, CASE
                        WHEN e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
                    END, CASE
                        WHEN e.sale_date IS NOT DISTINCT FROM a.sale_date THEN '"sale_date",'
                    END, CASE
                        WHEN e.shop_id IS NOT DISTINCT FROM a.shop_id THEN '"shop_id",'
                    END, CASE
                        WHEN e.price IS NOT DISTINCT FROM a.price THEN '"price",'
                    END) AS interlink__remarks
                FROM
                    _expect AS e
                    INNER JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
                WHERE
                    false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.interlink__remarks
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateValidationDatasourceSelectQuery_verbose()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var env = new SystemEnvironment();
		env.DbEnvironment.NullSafeEqualityOperator = string.Empty;
		var proxy = new ValidationMaterializer(env).AsPrivateProxy();

		var query = proxy.CreateValidationDatasourceSelectQuery(datasource, request);

		var expect = """
/* reverse only */
WITH
    _expect AS (
        /* inject request material filter */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks
        FROM
            (
                /* destination */
                SELECT
                    d.sale_journal_id,
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.price,
                    d.remarks
                FROM
                    sale_journals AS d
            ) AS d
            INNER JOIN __validation_request AS rm ON d.sale_journal_id = rm.sale_journal_id
    ),
    _actual AS (
        /* inject request material filter */
        /* does not exist if physically deleted */
        SELECT
            rm.sale_journal_id,
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
            INNER JOIN __validation_request AS rm ON d.sale_id = rm.sale_id
    )
SELECT
    e.sale_journal_id,
    a.sale_id,
    '{"deleted":true}' AS interlink__remarks
FROM
    _expect AS e
    LEFT JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
WHERE
    a.sale_id IS null
UNION ALL
/* reverse and additional */
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"updated":[', SUBSTRING(d.interlink__remarks, 1, LENGTH(d.interlink__remarks) - 1), ']}') AS interlink__remarks
FROM
    (
        SELECT
            e.sale_journal_id,
            a.sale_id,
            CONCAT(CASE
                WHEN e.sale_journal_id <> a.sale_journal_id OR (e.sale_journal_id IS NOT null AND a.sale_journal_id IS null) OR (e.sale_journal_id IS null AND a.sale_journal_id IS NOT null) THEN '"sale_journal_id",'
            END, CASE
                WHEN e.journal_closing_date <> a.journal_closing_date OR (e.journal_closing_date IS NOT null AND a.journal_closing_date IS null) OR (e.journal_closing_date IS null AND a.journal_closing_date IS NOT null) THEN '"journal_closing_date",'
            END, CASE
                WHEN e.sale_date <> a.sale_date OR (e.sale_date IS NOT null AND a.sale_date IS null) OR (e.sale_date IS null AND a.sale_date IS NOT null) THEN '"sale_date",'
            END, CASE
                WHEN e.shop_id <> a.shop_id OR (e.shop_id IS NOT null AND a.shop_id IS null) OR (e.shop_id IS null AND a.shop_id IS NOT null) THEN '"shop_id",'
            END, CASE
                WHEN e.price <> a.price OR (e.price IS NOT null AND a.price IS null) OR (e.price IS null AND a.price IS NOT null) THEN '"price",'
            END) AS interlink__remarks
        FROM
            _expect AS e
            INNER JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            false OR e.sale_journal_id <> a.sale_journal_id OR (e.sale_journal_id IS NOT null AND a.sale_journal_id IS null) OR (e.sale_journal_id IS null AND a.sale_journal_id IS NOT null) OR e.journal_closing_date <> a.journal_closing_date OR (e.journal_closing_date IS NOT null AND a.journal_closing_date IS null) OR (e.journal_closing_date IS null AND a.journal_closing_date IS NOT null) OR e.sale_date <> a.sale_date OR (e.sale_date IS NOT null AND a.sale_date IS null) OR (e.sale_date IS null AND a.sale_date IS NOT null) OR e.shop_id <> a.shop_id OR (e.shop_id IS NOT null AND a.shop_id IS null) OR (e.shop_id IS null AND a.shop_id IS NOT null) OR e.price <> a.price OR (e.price IS NOT null AND a.price IS null) OR (e.price IS null AND a.price IS NOT null)
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDatasourceMaterialQuery_CTE()
	{
		var datasource = DatasourceRepository.cte_sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationMaterialQuery(datasource, request, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    _expect AS (
        /* inject request material filter */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks
        FROM
            (
                /* destination */
                SELECT
                    d.sale_journal_id,
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.price,
                    d.remarks
                FROM
                    sale_journals AS d
            ) AS d
            INNER JOIN __validation_request AS rm ON d.sale_journal_id = rm.sale_journal_id
    ),
    __raw AS (
        /* inject request material filter */
        SELECT
            s.sale_date AS journal_closing_date,
            s.sale_date,
            s.shop_id,
            s.price,
            s.sale_id,
            s.sale_detail_id,
            rm.sale_journal_id
        FROM
            sale_detail AS s
            INNER JOIN __validation_request AS rm ON s.sale_id = rm.sale_id
    ),
    _actual AS (
        /* inject request material filter */
        /* does not exist if physically deleted */
        SELECT
            rm.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.sale_id
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
            INNER JOIN __validation_request AS rm ON d.sale_id = rm.sale_id
    ),
    _target_datasource AS (
        /* reverse only */
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS interlink__remarks
        FROM
            _expect AS e
            LEFT JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            a.sale_id IS null
        UNION ALL
        /* reverse and additional */
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"updated":[', SUBSTRING(d.interlink__remarks, 1, LENGTH(d.interlink__remarks) - 1), ']}') AS interlink__remarks
        FROM
            (
                SELECT
                    e.sale_journal_id,
                    a.sale_id,
                    CONCAT(CASE
                        WHEN e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
                    END, CASE
                        WHEN e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
                    END, CASE
                        WHEN e.sale_date IS NOT DISTINCT FROM a.sale_date THEN '"sale_date",'
                    END, CASE
                        WHEN e.shop_id IS NOT DISTINCT FROM a.shop_id THEN '"shop_id",'
                    END, CASE
                        WHEN e.price IS NOT DISTINCT FROM a.price THEN '"price",'
                    END) AS interlink__remarks
                FROM
                    _expect AS e
                    INNER JOIN _actual AS a ON e.sale_journal_id = a.sale_journal_id
                WHERE
                    false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.interlink__remarks
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestToAdditionalRequestMaterial()
	{
		var material = MaterialRepository.ValidationMaterial;
		var additional = material.ToAdditionalRequestMaterial();
		var query = additional.SelectQuery;

		var expect = """
/* since the keymap is assumed to have been deleted in the reverses process, we will not check its existence here. */
SELECT
    d.sale_id,
    r.root__sale_journal_id,
    r.origin__sale_journal_id,
    d.interlink__remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.sale_id,
            t.interlink__remarks
        FROM
            __validation_datasource AS t
    ) AS d
    INNER JOIN sale_journals__reverse AS r ON d.sale_journal_id = r.origin__sale_journal_id
WHERE
    d.sale_id IS NOT null
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestToReverseRequestMaterial()
	{
		var material = MaterialRepository.ValidationMaterial;
		var reverse = material.ToReverseRequestMaterial();
		var query = reverse.SelectQuery;

		var expect = """
SELECT
    d.sale_journal_id,
    d.interlink__remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.sale_id,
            t.interlink__remarks
        FROM
            __validation_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
