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
    p.key_map,
    p.key_relation
FROM
    sale_journals__r__reverse AS d
    INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.sale_journal_id
    LEFT JOIN sale_journals__relation AS reverse ON r.sale_journal_id = reverse.origin__sale_journal_id
    INNER JOIN interlink_process AS p ON r.interlink__process_id = p.interlink__process_id
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

	//	[Fact]
	//	public void TestCleanUpMaterialRequestQuery()
	//	{
	//		var destination = DestinationRepository.sale_journals;
	//		var requestMaterial = MaterialRepository.ReverseRequestMeterial;

	//		var query = Proxy.CleanUpMaterialRequestQuery(destination, requestMaterial);

	//		var expect = """
	//DELETE FROM
	//    __reverse_request AS d
	//WHERE
	//    (d.sale_journal_id) IN (
	//        /* Delete duplicate rows so that the destination ID is unique */
	//        SELECT
	//            r.sale_journal_id
	//        FROM
	//            __reverse_request AS r
	//        WHERE
	//            r.row_num <> 1
	//    )
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

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
            rm.key_map,
            rm.key_relation,
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
    d.key_map,
    d.key_relation,
    d.interlink__remarks
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
