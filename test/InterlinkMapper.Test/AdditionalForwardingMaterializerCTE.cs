using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class AdditionalForwardingMaterializerCTE
{
	public AdditionalForwardingMaterializerCTE(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new AdditionalForwardingMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly MaterializeServiceProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void TestCreateMaterialQuery_Has_raw_CTE()
	{
		var datasource = DatasourceRepository.cte_sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

		var query = Proxy.CreateAdditionalMaterialQuery(datasource, requestMaterial, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __datasource
AS
WITH
    __raw AS (
        /* inject request material filter */
        SELECT
            s.sale_date AS journal_closing_date,
            s.sale_date,
            s.shop_id,
            s.price,
            s.sale_id,
            s.sale_detail_id
        FROM
            sale_detail AS s
            INNER JOIN __additional_request AS rm ON s.sale_id = rm.sale_id
    ),
    _target_datasource AS (
        /* inject request material filter */
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
            INNER JOIN __additional_request AS rm ON d.sale_id = rm.sale_id
    )
SELECT
    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.sale_id
FROM
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
