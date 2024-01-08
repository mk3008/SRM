using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ReverseDatasourceMaterializerTest
{
	public ReverseDatasourceMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ReverseDatasourceMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseDatasourceMaterializerProxy Proxy;

	public InterlinkProcess ProcessRow => SystemRepository.GetDummyProcess(DatasourceRepository.sales);

	[Fact]
	public void TestCreateMaterialQuery()
	{
		var destination = DestinationRepository.sale_journals;
		var requestMaterial = MaterialRepository.ReverseRequestMeterial;

		var query = Proxy.CreateReverseMaterialQuery(destination, requestMaterial);

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
}
