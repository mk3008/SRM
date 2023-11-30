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
	public void TestCreateRequestMaterialTableQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialTableQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_request
AS
SELECT
    r.sale_journals__rv_sales_id,
    r.sale_id,
    r.created_at,
    m.sale_journal_id,
    ROW_NUMBER() OVER(
        PARTITION BY
            m.sale_journal_id
        ORDER BY
            r.sale_journals__rv_sales_id
    ) AS row_num
FROM
    sale_journals__rv_sales AS r
    INNER JOIN sale_journals__m_sales AS m ON r.sale_id = m.sale_id

""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(requestMaterial, datasource);

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
	public void TestCleanUpMaterialRequestQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CleanUpMaterialRequestQuery(requestMaterial, datasource);

		var expect = """
DELETE FROM
    __validation_request AS d
WHERE
    (d.sale_journals__rv_sales_id) IN (
        /* Delete duplicate rows so that the destination ID is unique */
        SELECT
            r.sale_journals__rv_sales_id
        FROM
            __validation_request AS r
        WHERE
            r.row_num <> 1
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
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateExpectValueSelectQuery(requestMaterial, datasource);

		var expect = """
/* expected value */
SELECT
    d.sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.remarks,
    m.sale_id
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
    INNER JOIN sale_journals__m_sales AS m ON d.sale_journal_id = m.sale_journal_id
WHERE
    EXISTS (
        /* exists request material */
        SELECT
            *
        FROM
            __validation_request AS x
        WHERE
            x.sale_id = m.sale_id
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateActualValueSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateActualValueSelectQuery(requestMaterial, datasource);

		var expect = """
/* actual value */
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
            __validation_request AS x
        WHERE
            x.sale_id = d.sale_id
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateValidationDatasourceSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationDatasourceSelectQuery(requestMaterial, datasource);

		var expect = """
WITH
    _expect AS (
        /* expected value */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks,
            m.sale_id
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
            INNER JOIN sale_journals__m_sales AS m ON d.sale_journal_id = m.sale_journal_id
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_id = m.sale_id
            )
    ),
    _actual AS (
        /* actual value */
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
                    __validation_request AS x
                WHERE
                    x.sale_id = d.sale_id
            )
    )
SELECT
    e.sale_journal_id,
    a.sale_id,
    '{"deleted":true}' AS remarks
FROM
    _expect AS e
    LEFT JOIN _actual AS a ON e.sale_id = a.sale_id
WHERE
    a.sale_id IS null
UNION ALL
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"changed":[', SUBSTRING(d.remarks, 1, LENGTH(d.remarks) - 1), ']}') AS remarks
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
            END) AS remarks
        FROM
            _expect AS e
            INNER JOIN _actual AS a ON e.sale_id = a.sale_id
        WHERE
            false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDatasourceMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationDatasourceMaterialQuery(requestMaterial, datasource, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    _expect AS (
        /* expected value */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks,
            m.sale_id
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
            INNER JOIN sale_journals__m_sales AS m ON d.sale_journal_id = m.sale_journal_id
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_id = m.sale_id
            )
    ),
    _actual AS (
        /* actual value */
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
                    __validation_request AS x
                WHERE
                    x.sale_id = d.sale_id
            )
    ),
    _target_datasource AS (
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS remarks
        FROM
            _expect AS e
            LEFT JOIN _actual AS a ON e.sale_id = a.sale_id
        WHERE
            a.sale_id IS null
        UNION ALL
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"changed":[', SUBSTRING(d.remarks, 1, LENGTH(d.remarks) - 1), ']}') AS remarks
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
                    END) AS remarks
                FROM
                    _expect AS e
                    INNER JOIN _actual AS a ON e.sale_id = a.sale_id
                WHERE
                    false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.remarks
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
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var env = new SystemEnvironment();
		env.DbEnvironment.NullSafeEqualityOperator = string.Empty;
		var proxy = new ValidationMaterializer(env).AsPrivateProxy();

		var query = proxy.CreateValidationDatasourceSelectQuery(requestMaterial, datasource);

		var expect = """
WITH
    _expect AS (
        /* expected value */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks,
            m.sale_id
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
            INNER JOIN sale_journals__m_sales AS m ON d.sale_journal_id = m.sale_journal_id
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_id = m.sale_id
            )
    ),
    _actual AS (
        /* actual value */
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
                    __validation_request AS x
                WHERE
                    x.sale_id = d.sale_id
            )
    )
SELECT
    e.sale_journal_id,
    a.sale_id,
    '{"deleted":true}' AS remarks
FROM
    _expect AS e
    LEFT JOIN _actual AS a ON e.sale_id = a.sale_id
WHERE
    a.sale_id IS null
UNION ALL
SELECT
    d.sale_journal_id,
    d.sale_id,
    CONCAT('{"changed":[', SUBSTRING(d.remarks, 1, LENGTH(d.remarks) - 1), ']}') AS remarks
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
            END) AS remarks
        FROM
            _expect AS e
            INNER JOIN _actual AS a ON e.sale_id = a.sale_id
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
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateValidationDatasourceMaterialQuery(requestMaterial, datasource, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_datasource
AS
WITH
    _expect AS (
        /* expected value */
        SELECT
            d.sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks,
            m.sale_id
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
            INNER JOIN sale_journals__m_sales AS m ON d.sale_journal_id = m.sale_journal_id
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_id = m.sale_id
            )
    ),
    __raw AS (
        /* request filter is injected */
        SELECT
            s.sale_date AS journal_closing_date,
            s.sale_date,
            s.shop_id,
            s.price,
            s.sale_id,
            s.sale_detail_id
        FROM
            sale_detail AS s
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __validation_request AS x
                WHERE
                    x.sale_id = s.sale_id
            )
    ),
    _actual AS (
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
    ),
    _target_datasource AS (
        SELECT
            e.sale_journal_id,
            a.sale_id,
            '{"deleted":true}' AS remarks
        FROM
            _expect AS e
            LEFT JOIN _actual AS a ON e.sale_id = a.sale_id
        WHERE
            a.sale_id IS null
        UNION ALL
        SELECT
            d.sale_journal_id,
            d.sale_id,
            CONCAT('{"changed":[', SUBSTRING(d.remarks, 1, LENGTH(d.remarks) - 1), ']}') AS remarks
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
                    END) AS remarks
                FROM
                    _expect AS e
                    INNER JOIN _actual AS a ON e.sale_id = a.sale_id
                WHERE
                    false OR e.sale_journal_id IS NOT DISTINCT FROM a.sale_journal_id OR e.journal_closing_date IS NOT DISTINCT FROM a.journal_closing_date OR e.sale_date IS NOT DISTINCT FROM a.sale_date OR e.shop_id IS NOT DISTINCT FROM a.shop_id OR e.price IS NOT DISTINCT FROM a.price
            ) AS d
    )
SELECT
    d.sale_journal_id,
    d.sale_id,
    d.remarks
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
