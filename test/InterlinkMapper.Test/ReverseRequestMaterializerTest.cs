using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ReverseRequestMaterializerTest
{
	public ReverseRequestMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ReverseRequestMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseRequestMaterializerProxy Proxy;

	public InterlinkProcess ProcessRow => SystemRepository.GetDummyProcess(DatasourceRepository.sales);

	[Fact]
	public void TestCreateRequestMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;

		var query = Proxy.CreateRequestMaterialQuery(destination, (SelectQuery x) => x);

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
	public void CreateCleanUpRequestMaterialQuery()
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
            (
                SELECT
                    t.sale_journals__req_reverse_id,
                    t.sale_journal_id,
                    t.root__sale_journal_id,
                    t.origin__sale_journal_id,
                    t.interlink_datasource_id
                FROM
                    __reverse_request AS t
            ) AS r
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
