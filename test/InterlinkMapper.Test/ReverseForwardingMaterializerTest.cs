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

	public InterlinkProcess ProcessRow => SystemRepository.GetDummyProcess(DatasourceRepository.sales);

	//	[Fact]
	//	public void TestCreateProcessRowSelectQuery()
	//	{
	//		var material = MaterialRepository.ReverseMeterial;

	//		var query = material.AsPrivateProxy().CreateProcessRowSelectQuery(1);

	//		var expect = """
	///*
	//  :InterlinkTransactionId = 1
	//  :ActionName = 'reverse'
	//*/
	//SELECT
	//    d.interlink_datasource_id AS InterlinkDatasourceId,
	//    d.interlink_key_map AS KeyMapTableName,
	//    d.interlink_key_relation AS KeyRelationTableName,
	//    :InterlinkTransactionId AS InterlinkTransactionId,
	//    :ActionName AS ActionName,
	//    COUNT(*) AS InsertCount
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.root__sale_journal_id,
	//            t.origin__sale_journal_id,
	//            t.journal_closing_date,
	//            t.sale_date,
	//            t.shop_id,
	//            t.price,
	//            t.remarks,
	//            t.interlink_datasource_id,
	//            t.interlink_key_map,
	//            t.interlink_key_relation,
	//            t.interlink_remarks
	//        FROM
	//            __reverse_datasource AS t
	//    ) AS d
	//GROUP BY
	//    d.interlink_datasource_id,
	//    d.interlink_key_map,
	//    d.interlink_key_relation
	//ORDER BY
	//    d.interlink_datasource_id,
	//    d.interlink_key_map,
	//    d.interlink_key_relation
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

	[Fact]
	public void TestCreateRequestMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;

		var query = Proxy.CreateRequestMaterialQuery(destination);

		var expect = """
CREATE TEMPORARY TABLE
    __reverse_request
AS
SELECT
    req.sale_journals__req_reverse_id,
    rel.sale_journal_id,
    rel.root__sale_journal_id,
    rel.origin__sale_journal_id,
    proc.interlink_datasource_id
FROM
    sale_journals__req_reverse AS req
    INNER JOIN sale_journals__relation AS rel ON req.sale_journal_id = rel.sale_journal_id
    INNER JOIN interlink.interlink_process AS proc ON rel.interlink_process_id = proc.interlink_process_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCelanUpRequestMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;
		var material = MaterialRepository.ReverseRequestMeterial;
		var query = Proxy.CreateCleanUpRequestMaterialQuery(material, destination);

		var expect = """
DELETE FROM
    __reverse_request AS d
WHERE
    (d.sale_journals__req_reverse_id) IN (
        /* Exclude irreversible data. */
        SELECT
            d.sale_journals__req_reverse_id
        FROM
            (
                SELECT
                    d.sale_journals__req_reverse_id,
                    d.origin__sale_journal_id,
                    d.sale_journal_id,
                    ROW_NUMBER() OVER(
                        PARTITION BY
                            d.root__sale_journal_id
                        ORDER BY
                            d.sale_journal_id DESC
                    ) AS row_num
                FROM
                    (
                        SELECT
                            t.sale_journals__req_reverse_id,
                            t.sale_journal_id,
                            t.root__sale_journal_id,
                            t.origin__sale_journal_id,
                            t.interlink_datasource_id
                        FROM
                            __reverse_request AS t
                    ) AS d
            ) AS d
        WHERE
            NOT (d.row_num = 1 AND d.origin__sale_journal_id = d.sale_journal_id)
    )
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
    reverse_data AS (
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
            'force' AS interlink_remarks
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
    d.interlink_remarks
FROM
    reverse_data AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	//	[Fact]
	//	public void TestCreateKeyMapDeleteQuery()
	//	{
	//		var material = MaterialRepository.ReverseMeterial;
	//		var query = material.AsPrivateProxy().CreateKeyMapDeleteQuery(ProcessRow);

	//		var expect = """
	//DELETE FROM
	//    sale_journals__key_m_sales AS d
	//WHERE
	//    (d.sale_journal_id) IN (
	//        SELECT
	//            d.origin__sale_journal_id AS sale_journal_id
	//        FROM
	//            (
	//                SELECT
	//                    d.sale_journal_id,
	//                    d.root__sale_journal_id,
	//                    d.origin__sale_journal_id,
	//                    d.journal_closing_date,
	//                    d.sale_date,
	//                    d.shop_id,
	//                    d.price,
	//                    d.remarks,
	//                    d.interlink_datasource_id,
	//                    d.interlink_remarks
	//                FROM
	//                    (
	//                        SELECT
	//                            t.sale_journal_id,
	//                            t.root__sale_journal_id,
	//                            t.origin__sale_journal_id,
	//                            t.journal_closing_date,
	//                            t.sale_date,
	//                            t.shop_id,
	//                            t.price,
	//                            t.remarks,
	//                            t.interlink_datasource_id,
	//                            t.interlink_remarks
	//                        FROM
	//                            __reverse_datasource AS t
	//                    ) AS d
	//                WHERE
	//                    d.interlink_datasource_id = 1
	//            ) AS d
	//    )
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

	//	[Fact]
	//	public void TestCreateKeyRelationInsertQuery()
	//	{
	//		var material = MaterialRepository.ReverseMeterial;
	//		var query = material.AsPrivateProxy().CreateKeyRelationInsertQuery(ProcessRow);

	//		var expect = """
	//INSERT INTO
	//    sale_journals__key_r_sales (
	//        interlink_process_id, sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink_remarks
	//    )
	//SELECT
	//    0 AS interlink_process_id,
	//    d.sale_journal_id,
	//    d.root__sale_journal_id,
	//    d.origin__sale_journal_id,
	//    d.interlink_remarks
	//FROM
	//    (
	//        SELECT
	//            d.sale_journal_id,
	//            d.root__sale_journal_id,
	//            d.origin__sale_journal_id,
	//            d.journal_closing_date,
	//            d.sale_date,
	//            d.shop_id,
	//            d.price,
	//            d.remarks,
	//            d.interlink_datasource_id,
	//            d.interlink_remarks
	//        FROM
	//            (
	//                SELECT
	//                    t.sale_journal_id,
	//                    t.root__sale_journal_id,
	//                    t.origin__sale_journal_id,
	//                    t.journal_closing_date,
	//                    t.sale_date,
	//                    t.shop_id,
	//                    t.price,
	//                    t.remarks,
	//                    t.interlink_datasource_id,
	//                    t.interlink_remarks
	//                FROM
	//                    __reverse_datasource AS t
	//            ) AS d
	//        WHERE
	//            d.interlink_datasource_id = 1
	//    ) AS d
	//ORDER BY
	//    d.sale_journal_id
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

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
