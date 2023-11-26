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
    r.created_at
FROM
    sale_journals__rv_sales AS r
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
                    __additional_request AS x
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
    (d.sale_journal_id) IN (
        /* If it does not exist in the keymap table, remove it from the target */
        SELECT
            r.sale_journal_id
        FROM
            __validation_request AS r
        WHERE
            NOT EXISTS (
                SELECT
                    *
                FROM
                    sale_journals__m_sales AS x
                WHERE
                    x.sale_journal_id = r.sale_journal_id
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
	public void TestCreateDiffSelectQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;

		var query = Proxy.CreateDiffSelectQuery(requestMaterial, datasource);

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
    CASE
        WHEN a.sale_id IS null THEN '{"deleted":true}'
        ELSE CONCAT('{"changed":[', CASE
            WHEN e.sale_journal_id <> a.sale_journal_id OR (e.sale_journal_id IS NOT null AND a.sale_journal_id IS null) OR (e.sale_journal_id IS null AND a.sale_journal_id IS NOT null) THEN '"sale_journal_id",'
        END, CASE
            WHEN e.journal_closing_date <> a.journal_closing_date OR (e.journal_closing_date IS NOT null AND a.journal_closing_date IS null) OR (e.journal_closing_date IS null AND a.journal_closing_date IS NOT null) THEN '"journal_closing_date",'
        END, CASE
            WHEN e.sale_date <> a.sale_date OR (e.sale_date IS NOT null AND a.sale_date IS null) OR (e.sale_date IS null AND a.sale_date IS NOT null) THEN '"sale_date",'
        END, CASE
            WHEN e.shop_id <> a.shop_id OR (e.shop_id IS NOT null AND a.shop_id IS null) OR (e.shop_id IS null AND a.shop_id IS NOT null) THEN '"shop_id",'
        END, CASE
            WHEN e.price <> a.price OR (e.price IS NOT null AND a.price IS null) OR (e.price IS null AND a.price IS NOT null) THEN '"price",'
        END, ']}')
    END AS remarks
FROM
    _expect AS e
    LEFT JOIN _actual AS a ON e.sale_id = a.sale_id
WHERE
    a.sale_id IS null OR e.sale_journal_id <> a.sale_journal_id OR (e.sale_journal_id IS NOT null AND a.sale_journal_id IS null) OR (e.sale_journal_id IS null AND a.sale_journal_id IS NOT null) OR e.journal_closing_date <> a.journal_closing_date OR (e.journal_closing_date IS NOT null AND a.journal_closing_date IS null) OR (e.journal_closing_date IS null AND a.journal_closing_date IS NOT null) OR e.sale_date <> a.sale_date OR (e.sale_date IS NOT null AND a.sale_date IS null) OR (e.sale_date IS null AND a.sale_date IS NOT null) OR e.shop_id <> a.shop_id OR (e.shop_id IS NOT null AND a.shop_id IS null) OR (e.shop_id IS null AND a.shop_id IS NOT null) OR e.price <> a.price OR (e.price IS NOT null AND a.price IS null) OR (e.price IS null AND a.price IS NOT null)
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
			m.sales_id
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
	        INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.sale_journal_id
	        INNER JOIN interlink_process AS p ON r.interlink__process_id = p.interlink__process_id
			INNER JOIN sale_journal__m_sales m on d.sale_journal_id = m.sale_journal_id
	    WHERE
	        EXISTS (
	            /* find transferred value from keymap */
	            SELECT
	                *
	            FROM
	                __validation_request AS x 
	            WHERE
					x.sales_id = m.sales_id
			)
	),
	_actual as (
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
                    s.sale_id,
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
					x.sales_id = d.sales_id
			)
	),
	_diff as (
		/* If a.sales_id is NULL, it has been deleted. */
		/* If not, the value has changed. */
		select
	        e.sale_journal_id,
			a.sales_id, 
			case 
				when a.sales_id is null then '{"deleted":true}'
				when 
					'{"changed":['
					||
					concat(
						case when d.journal_closing_date <> a.journal_closing_date then '"journal_closing_date",' end,
						case when d.sale_date <> a.sale_date then '"sale_date",' end,
						case when d.shop_id <> a.shop_id then '"shop_id",' end,
						case when d.price <> a.price then '"price",' end
					)
					|| ']}'
				end
			end	as remarks
		from
			_expect e
			left join _actual a on e.sales_id = a.sales_id
		where
				a.sales_id is null
			or	d.journal_closing_date <> a.journal_closing_date
			or	d.sale_date <> a.sale_date
			or	d.shop_id <> a.shop_id
			or	d.price <> a.price
	)
SELECT
	NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
	d.journal_closing_date,
	d.sale_date,
	d.shop_id,
	d.price,
	d.remarks,
	d.keymap_name
FROM
	_target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
