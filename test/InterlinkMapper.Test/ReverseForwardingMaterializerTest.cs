using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ReverseForwardingMaterializerTest
{
	public ReverseForwardingMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ReverseForwardingMaterializer(Environment).AsPrivateProxy();
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly ReverseForwardingMaterializerProxy Proxy;

	private DbDestination GetTestDestination()
	{
		return DestinationRepository.sale_journals;
	}

	private MaterializeResult GetDummyRequestMeterial()
	{
		return new MaterializeResult()
		{
			MaterialName = "__reverse_request",
		};
	}

	[Fact]
	public void TestCreateRequestMaterialTableQuery()
	{
		var destination = GetTestDestination();
		var query = Proxy.CreateRequestMaterialTableQuery(destination);

		var expect = """
CREATE TEMPORARY TABLE
    __reverse_request
AS
SELECT
    r.sale_journals__request_reverse_id,
    r.sale_journal_id,
    r.created_at
FROM
    sale_journals__request_reverse AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var destination = GetTestDestination();
		var requestMaterial = GetDummyRequestMeterial();
		var query = Proxy.CreateOriginDeleteQuery(requestMaterial, destination);

		var expect = """
DELETE FROM
    sale_journals__request_reverse AS d
WHERE
    (d.sale_journals__request_reverse_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__request_reverse_id
        FROM
            sale_journals__request_reverse AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __reverse_request AS x
                WHERE
                    x.sale_journals__request_reverse_id = r.sale_journals__request_reverse_id
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
		var destination = GetTestDestination();
		var requestMaterial = GetDummyRequestMeterial();
		var query = Proxy.CleanUpMaterialRequestQuery(requestMaterial, destination);

		var expect = """
DELETE FROM
    __reverse_request AS d
WHERE
    (d.sale_journal_id) IN (
        /* If it does not exist in the relation table, remove it from the target */
        SELECT
            r.sale_journal_id
        FROM
            __reverse_request AS r
        WHERE
            NOT EXISTS (
                SELECT
                    *
                FROM
                    sale_journals__relation AS x
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
	public void TestCreateDatasourceMaterialQuery()
	{
		var destination = GetTestDestination();

		var requestMaterial = GetDummyRequestMeterial();
		var query = Proxy.CreateReverseDatasourceMaterialQuery(requestMaterial, destination, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __reverse_datasource
AS
WITH
    _target_datasource AS (
        /* data source to be added */
        SELECT
            d.sale_journal_id AS origin_sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price * -1 AS price,
            d.remarks,
            p.keymap_name
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
            INNER JOIN interlink_process AS p ON r.interlink_process_id = p.interlink_process_id
        WHERE
            EXISTS (
                /* exists request material */
                SELECT
                    *
                FROM
                    __reverse_request AS x
                WHERE
                    x.sale_journal_id = d.sale_journal_id
            )
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.origin_sale_journal_id,
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
