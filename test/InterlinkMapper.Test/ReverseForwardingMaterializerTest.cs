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

		Proxy = new ReverseMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseMaterializerProxy Proxy;

	public InterlinkProcessRow ProcessRow => GetProcessRow();

	private InterlinkProcessRow GetProcessRow()
	{
		var map = Environment.GetKeyMapTable(DatasourceRepository.sales);
		var relation = Environment.GetKeyRelationTable(DatasourceRepository.sales);

		return new InterlinkProcessRow
		{
			ActionName = "test",
			InterlinkDatasourceId = 1,
			InterlinkDestinationId = 2,
			InsertCount = 3,
			KeyMapTableName = map.Definition.TableFullName,
			KeyRelationTableName = relation.Definition.TableFullName,
			InterlinkProcessId = 4,
			InterlinkTransactionId = 5
		};
	}

	[Fact]
	public void TestCreateProcessRowSelectQuery()
	{
		var material = MaterialRepository.ReverseMeterial;

		var query = material.AsPrivateProxy().CreateProcessRowSelectQuery(1);

		var expect = """
/*
  :InterlinkTransactionId = 1
  :ActionName = 'reverse'
*/
SELECT
    d.interlink_datasource_id AS InterlinkDatasourceId,
    d.interlink_destination_id AS InterlinkDestinationId,
    d.interlink_key_map AS KeyMapTableName,
    d.interlink_key_relation AS KeyRelationTableName,
    :InterlinkTransactionId AS InterlinkTransactionId,
    :ActionName AS ActionName,
    COUNT(*) AS InsertCount
FROM
    (
        SELECT
            t.sale_journal_id,
            t.root__sale_journal_id,
            t.origin__sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.remarks,
            t.interlink_datasource_id,
            t.interlink_destination_id,
            t.interlink_key_map,
            t.interlink_key_relation,
            t.interlink_remarks
        FROM
            __reverse_datasource AS t
    ) AS d
GROUP BY
    d.interlink_datasource_id,
    d.interlink_destination_id,
    d.interlink_key_map,
    d.interlink_key_relation
ORDER BY
    d.interlink_datasource_id,
    d.interlink_destination_id,
    d.interlink_key_map,
    d.interlink_key_relation
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateRequestMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;

		var query = Proxy.CreateRequestMaterialQuery(destination);

		var expect = """
CREATE TEMPORARY TABLE
    __reverse_request
AS
/* Only original slips can be reversed.(where id = origin_id) */
/* Only unprocessed slips can be reversed.(where reverse is null) */
SELECT
    r.sale_journals__req_reverse_id,
    r.sale_journal_id,
    r.root__sale_journal_id,
    r.interlink_remarks,
    p.interlink_datasource_id,
    p.interlink_destination_id,
    p.interlink_key_map,
    p.interlink_key_relation
FROM
    sale_journals__req_reverse AS d
    INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.sale_journal_id
    LEFT JOIN sale_journals__relation AS reverse ON r.sale_journal_id = reverse.origin__sale_journal_id
    INNER JOIN interlink_process AS p ON r.interlink_process_id = p.interlink_process_id
WHERE
    r.sale_journal_id = r.origin__sale_journal_id
    AND reverse.sale_journal_id IS null
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var destination = DestinationRepository.sale_journals;
		var requestMaterial = MaterialRepository.ReverseRequestMeterial;

		var query = Proxy.CreateOriginDeleteQuery(destination, requestMaterial);

		var expect = """
 DELETE FROM
    sale_journals__req_reverse AS d
WHERE
    (d.sale_journals__req_reverse_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__req_reverse_id
        FROM
            sale_journals__req_reverse AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __reverse_request AS x
                WHERE
                    x.sale_journals__req_reverse_id = r.sale_journals__req_reverse_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;
		var requestMaterial = MaterialRepository.ReverseRequestMeterial;

		var query = Proxy.CreateReverseMaterialQuery(destination, requestMaterial, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __reverse_datasource
AS
WITH
    _target_datasource AS (
        /* data source to be added */
        SELECT
            rm.root__sale_journal_id,
            d.sale_journal_id AS origin__sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price * -1 AS price,
            d.remarks,
            rm.interlink_datasource_id,
            rm.interlink_destination_id,
            rm.interlink_key_map,
            rm.interlink_key_relation,
            rm.interlink_remarks
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
            INNER JOIN __reverse_request AS rm ON d.sale_journal_id = rm.sale_journal_id
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.remarks,
    d.interlink_datasource_id,
    d.interlink_destination_id,
    d.interlink_key_map,
    d.interlink_key_relation,
    d.interlink_remarks
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateKeyMapDeleteQuery()
	{
		var material = MaterialRepository.ReverseMeterial;
		var query = material.AsPrivateProxy().CreateMapDeleteQuery(ProcessRow);

		var expect = """
/*
  :interlink_datasource_id = 1
  :interlink_destination_id = 2
  :interlink_key_map = 'sale_journals__key_m_sales'
  :interlink_key_relation = 'sale_journals__key_r_sales'
*/
DELETE FROM
    sale_journals__key_m_sales AS d
WHERE
    (d.sale_journal_id) IN (
        SELECT
            d.origin__sale_journal_id AS sale_journal_id
        FROM
            (
                SELECT
                    d.sale_journal_id,
                    d.root__sale_journal_id,
                    d.origin__sale_journal_id,
                    d.journal_closing_date,
                    d.sale_date,
                    d.shop_id,
                    d.price,
                    d.remarks,
                    d.interlink_datasource_id,
                    d.interlink_destination_id,
                    d.interlink_key_map,
                    d.interlink_key_relation,
                    d.interlink_remarks
                FROM
                    (
                        SELECT
                            t.sale_journal_id,
                            t.root__sale_journal_id,
                            t.origin__sale_journal_id,
                            t.journal_closing_date,
                            t.sale_date,
                            t.shop_id,
                            t.price,
                            t.remarks,
                            t.interlink_datasource_id,
                            t.interlink_destination_id,
                            t.interlink_key_map,
                            t.interlink_key_relation,
                            t.interlink_remarks
                        FROM
                            __reverse_datasource AS t
                    ) AS d
                WHERE
                    d.interlink_datasource_id = :interlink_datasource_id
                    AND d.interlink_destination_id = :interlink_destination_id
                    AND d.interlink_key_map = :interlink_key_map
                    AND d.interlink_key_relation = :interlink_key_relation
            ) AS d
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateKeyRelationInsertQuery()
	{
		var material = MaterialRepository.ReverseMeterial;
		var query = material.AsPrivateProxy().CreateRelationInsertQuery(ProcessRow);

		var expect = """
/*
  :interlink_datasource_id = 1
  :interlink_destination_id = 2
  :interlink_key_map = 'sale_journals__key_m_sales'
  :interlink_key_relation = 'sale_journals__key_r_sales'
  :interlink_process_id = 4
*/
INSERT INTO
    sale_journals__key_r_sales (
        interlink_process_id, sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink_remarks
    )
SELECT
    :interlink_process_id AS interlink_process_id,
    d.sale_journal_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id,
    d.interlink_remarks
FROM
    (
        SELECT
            d.sale_journal_id,
            d.root__sale_journal_id,
            d.origin__sale_journal_id,
            d.journal_closing_date,
            d.sale_date,
            d.shop_id,
            d.price,
            d.remarks,
            d.interlink_datasource_id,
            d.interlink_destination_id,
            d.interlink_key_map,
            d.interlink_key_relation,
            d.interlink_remarks
        FROM
            (
                SELECT
                    t.sale_journal_id,
                    t.root__sale_journal_id,
                    t.origin__sale_journal_id,
                    t.journal_closing_date,
                    t.sale_date,
                    t.shop_id,
                    t.price,
                    t.remarks,
                    t.interlink_datasource_id,
                    t.interlink_destination_id,
                    t.interlink_key_map,
                    t.interlink_key_relation,
                    t.interlink_remarks
                FROM
                    __reverse_datasource AS t
            ) AS d
        WHERE
            d.interlink_datasource_id = :interlink_datasource_id
            AND d.interlink_destination_id = :interlink_destination_id
            AND d.interlink_key_map = :interlink_key_map
            AND d.interlink_key_relation = :interlink_key_relation
    ) AS d
ORDER BY
    d.sale_journal_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateDestinationInsertQuery()
	{
		var material = MaterialRepository.ReverseMeterial;
		var query = ((MaterializeResult)material).AsPrivateProxy().CreateDestinationInsertQuery();

		var expect = """
INSERT INTO
    sale_journals (
        sale_journal_id, journal_closing_date, sale_date, shop_id, price, remarks
    )
SELECT
    d.sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.root__sale_journal_id,
            t.origin__sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.remarks,
            t.interlink_datasource_id,
            t.interlink_destination_id,
            t.interlink_key_map,
            t.interlink_key_relation,
            t.interlink_remarks
        FROM
            __reverse_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
