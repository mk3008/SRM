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
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseForwardingMaterializerProxy Proxy;

	public ProcessRow ProcessRow => GetProcessRow();

	private ProcessRow GetProcessRow()
	{
		var map = Environment.GetKeyMapTable(DatasourceRepository.sales);
		var relation = Environment.GetKeyRelationTable(DatasourceRepository.sales);

		return new ProcessRow
		{
			ActionName = "test",
			DatasourceId = 1,
			DestinationId = 2,
			InsertCount = 3,
			KeyMapTableName = map.Definition.TableFullName,
			KeyRelationTableName = relation.Definition.TableFullName,
			ProcessId = 4,
			TransactionId = 5
		};
	}

	[Fact]
	public void TestCreateProcessRowSelectQuery()
	{
		var material = MaterialRepository.ReverseMeterial;

		var query = material.AsPrivateProxy().CreateProcessRowSelectQuery(1);

		var expect = """
/*
  :TransactionId = 1
  :ActionName = 'reverse'
*/
SELECT
    d.interlink__datasource_id AS DatasourceId,
    d.interlink__destination_id AS DestinationId,
    d.interlink__key_map AS KeyMapTableName,
    d.interlink__key_relation AS KeyRelationTableName,
    :TransactionId AS TransactionId,
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
            t.interlink__datasource_id,
            t.interlink__destination_id,
            t.interlink__key_map,
            t.interlink__key_relation,
            t.interlink__remarks
        FROM
            __reverse_datasource AS t
    ) AS d
GROUP BY
    d.interlink__datasource_id,
    d.interlink__destination_id,
    d.interlink__key_map,
    d.interlink__key_relation
ORDER BY
    d.interlink__datasource_id,
    d.interlink__destination_id,
    d.interlink__key_map,
    d.interlink__key_relation
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
    r.sale_journals__r__reverse_id,
    r.sale_journal_id,
    r.root__sale_journal_id,
    r.interlink__remarks,
    p.interlink__datasource_id,
    p.interlink__destination_id,
    p.interlink__key_map,
    p.interlink__key_relation
FROM
    sale_journals__r__reverse AS d
    INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.sale_journal_id
    LEFT JOIN sale_journals__relation AS reverse ON r.sale_journal_id = reverse.origin__sale_journal_id
    INNER JOIN interlink__process AS p ON r.interlink__process_id = p.interlink__process_id
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
    sale_journals__r__reverse AS d
WHERE
    (d.sale_journals__r__reverse_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__r__reverse_id
        FROM
            sale_journals__r__reverse AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __reverse_request AS x
                WHERE
                    x.sale_journals__r__reverse_id = r.sale_journals__r__reverse_id
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
            rm.interlink__datasource_id,
            rm.interlink__destination_id,
            rm.interlink__key_map,
            rm.interlink__key_relation,
            rm.interlink__remarks
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
    d.interlink__datasource_id,
    d.interlink__destination_id,
    d.interlink__key_map,
    d.interlink__key_relation,
    d.interlink__remarks
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
  :interlink__datasource_id = 1
  :interlink__destination_id = 2
  :interlink__key_map = 'sale_journals__km_sales'
  :interlink__key_relation = 'sale_journals__kr_sales'
*/
DELETE FROM
    sale_journals__km_sales AS d
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
                    d.interlink__datasource_id,
                    d.interlink__destination_id,
                    d.interlink__key_map,
                    d.interlink__key_relation,
                    d.interlink__remarks
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
                            t.interlink__datasource_id,
                            t.interlink__destination_id,
                            t.interlink__key_map,
                            t.interlink__key_relation,
                            t.interlink__remarks
                        FROM
                            __reverse_datasource AS t
                    ) AS d
                WHERE
                    d.interlink__datasource_id = :interlink__datasource_id
                    AND d.interlink__destination_id = :interlink__destination_id
                    AND d.interlink__key_map = :interlink__key_map
                    AND d.interlink__key_relation = :interlink__key_relation
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
  :interlink__datasource_id = 1
  :interlink__destination_id = 2
  :interlink__key_map = 'sale_journals__km_sales'
  :interlink__key_relation = 'sale_journals__kr_sales'
  :interlink__process_id = 4
*/
INSERT INTO
    sale_journals__kr_sales (
        interlink__process_id, sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink__remarks
    )
SELECT
    :interlink__process_id AS interlink__process_id,
    d.sale_journal_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id,
    d.interlink__remarks
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
            d.interlink__datasource_id,
            d.interlink__destination_id,
            d.interlink__key_map,
            d.interlink__key_relation,
            d.interlink__remarks
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
                    t.interlink__datasource_id,
                    t.interlink__destination_id,
                    t.interlink__key_map,
                    t.interlink__key_relation,
                    t.interlink__remarks
                FROM
                    __reverse_datasource AS t
            ) AS d
        WHERE
            d.interlink__datasource_id = :interlink__datasource_id
            AND d.interlink__destination_id = :interlink__destination_id
            AND d.interlink__key_map = :interlink__key_map
            AND d.interlink__key_relation = :interlink__key_relation
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
            t.interlink__datasource_id,
            t.interlink__destination_id,
            t.interlink__key_map,
            t.interlink__key_relation,
            t.interlink__remarks
        FROM
            __reverse_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
