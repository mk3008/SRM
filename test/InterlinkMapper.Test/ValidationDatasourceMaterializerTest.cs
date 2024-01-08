using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationDatasourceMaterializerTest
{
	public ValidationDatasourceMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ValidationDatasourceMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly ValidationDatasourceMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void CreateExpectValueSelectQuery()
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
	public void CreateActualValueSelectQuery()
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
	public void CreateValidationDatasourceSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationDatasourceSelectQuery(datasource, request);

		var expect = """
/* reverse only */
WITH
    expect_data AS (
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
    actual_data AS (
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
    '{"deleted":true}' AS interlink_remarks
FROM
    expect_data AS e
    LEFT JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
WHERE
    a.sale_id IS null
UNION ALL
/* reverse and additional */
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"updated":[', SUBSTRING(d.interlink_remarks, 1, LENGTH(d.interlink_remarks) - 1), ']}') AS interlink_remarks
FROM
    (
        SELECT
            e.sale_journal_id,
            a.sale_id,
            CONCAT(CASE
                WHEN e.sale_journal_id IS DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
            END, CASE
                WHEN e.journal_closing_date IS DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
            END, CASE
                WHEN e.sale_date IS DISTINCT FROM a.sale_date THEN '"sale_date",'
            END, CASE
                WHEN e.shop_id IS DISTINCT FROM a.shop_id THEN '"shop_id",'
            END, CASE
                WHEN e.price IS DISTINCT FROM a.price THEN '"price",'
            END) AS interlink_remarks
        FROM
            expect_data AS e
            INNER JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            false OR e.sale_journal_id IS DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS DISTINCT FROM a.journal_closing_date OR e.sale_date IS DISTINCT FROM a.sale_date OR e.shop_id IS DISTINCT FROM a.shop_id OR e.price IS DISTINCT FROM a.price
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateValidationMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationMaterialQuery(datasource, request);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    expect_data AS (
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
    actual_data AS (
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
    diff_data AS (
        /* reverse only */
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS interlink_remarks
        FROM
            expect_data AS e
            LEFT JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            a.sale_id IS null
        UNION ALL
        /* reverse and additional */
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"updated":[', SUBSTRING(d.interlink_remarks, 1, LENGTH(d.interlink_remarks) - 1), ']}') AS interlink_remarks
        FROM
            (
                SELECT
                    e.sale_journal_id,
                    a.sale_id,
                    CONCAT(CASE
                        WHEN e.sale_journal_id IS DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
                    END, CASE
                        WHEN e.journal_closing_date IS DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
                    END, CASE
                        WHEN e.sale_date IS DISTINCT FROM a.sale_date THEN '"sale_date",'
                    END, CASE
                        WHEN e.shop_id IS DISTINCT FROM a.shop_id THEN '"shop_id",'
                    END, CASE
                        WHEN e.price IS DISTINCT FROM a.price THEN '"price",'
                    END) AS interlink_remarks
                FROM
                    expect_data AS e
                    INNER JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
                WHERE
                    false OR e.sale_journal_id IS DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS DISTINCT FROM a.journal_closing_date OR e.sale_date IS DISTINCT FROM a.sale_date OR e.shop_id IS DISTINCT FROM a.shop_id OR e.price IS DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.interlink_remarks
FROM
    diff_data AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateValidationDatasourceSelectQuery_verbose()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var env = new SystemEnvironment();
		env.DbEnvironment.NullSafeEqualityOperator = string.Empty;
		var proxy = new ValidationDatasourceMaterializer(env).AsPrivateProxy();

		var query = proxy.CreateValidationDatasourceSelectQuery(datasource, request);

		var expect = """
/* reverse only */
WITH
    expect_data AS (
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
    actual_data AS (
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
    '{"deleted":true}' AS interlink_remarks
FROM
    expect_data AS e
    LEFT JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
WHERE
    a.sale_id IS null
UNION ALL
/* reverse and additional */
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"updated":[', SUBSTRING(d.interlink_remarks, 1, LENGTH(d.interlink_remarks) - 1), ']}') AS interlink_remarks
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
            END) AS interlink_remarks
        FROM
            expect_data AS e
            INNER JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            false OR e.sale_journal_id <> a.sale_journal_id OR (e.sale_journal_id IS NOT null AND a.sale_journal_id IS null) OR (e.sale_journal_id IS null AND a.sale_journal_id IS NOT null) OR e.journal_closing_date <> a.journal_closing_date OR (e.journal_closing_date IS NOT null AND a.journal_closing_date IS null) OR (e.journal_closing_date IS null AND a.journal_closing_date IS NOT null) OR e.sale_date <> a.sale_date OR (e.sale_date IS NOT null AND a.sale_date IS null) OR (e.sale_date IS null AND a.sale_date IS NOT null) OR e.shop_id <> a.shop_id OR (e.shop_id IS NOT null AND a.shop_id IS null) OR (e.shop_id IS null AND a.shop_id IS NOT null) OR e.price <> a.price OR (e.price IS NOT null AND a.price IS null) OR (e.price IS null AND a.price IS NOT null)
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateValidationMaterialQuery_CTE()
	{
		var datasource = DatasourceRepository.cte_sales;
		var request = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationMaterialQuery(datasource, request);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    expect_data AS (
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
    actual_data AS (
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
    diff_data AS (
        /* reverse only */
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS interlink_remarks
        FROM
            expect_data AS e
            LEFT JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
        WHERE
            a.sale_id IS null
        UNION ALL
        /* reverse and additional */
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"updated":[', SUBSTRING(d.interlink_remarks, 1, LENGTH(d.interlink_remarks) - 1), ']}') AS interlink_remarks
        FROM
            (
                SELECT
                    e.sale_journal_id,
                    a.sale_id,
                    CONCAT(CASE
                        WHEN e.sale_journal_id IS DISTINCT FROM a.sale_journal_id THEN '"sale_journal_id",'
                    END, CASE
                        WHEN e.journal_closing_date IS DISTINCT FROM a.journal_closing_date THEN '"journal_closing_date",'
                    END, CASE
                        WHEN e.sale_date IS DISTINCT FROM a.sale_date THEN '"sale_date",'
                    END, CASE
                        WHEN e.shop_id IS DISTINCT FROM a.shop_id THEN '"shop_id",'
                    END, CASE
                        WHEN e.price IS DISTINCT FROM a.price THEN '"price",'
                    END) AS interlink_remarks
                FROM
                    expect_data AS e
                    INNER JOIN actual_data AS a ON e.sale_journal_id = a.sale_journal_id
                WHERE
                    false OR e.sale_journal_id IS DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS DISTINCT FROM a.journal_closing_date OR e.sale_date IS DISTINCT FROM a.sale_date OR e.shop_id IS DISTINCT FROM a.shop_id OR e.price IS DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.interlink_remarks
FROM
    diff_data AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void ToAdditionalRequestMaterial()
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
    d.interlink_remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.sale_id,
            t.interlink_remarks
        FROM
            __validation_datasource AS t
    ) AS d
    INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.origin__sale_journal_id
WHERE
    d.sale_id IS NOT null
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void ToReverseRequestMaterial()
	{
		var material = MaterialRepository.ValidationMaterial;
		var reverse = material.ToReverseRequestMaterial();
		var query = reverse.SelectQuery;

		var expect = """
SELECT
    d.sale_journal_id,
    d.interlink_remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.sale_id,
            t.interlink_remarks
        FROM
            __validation_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
