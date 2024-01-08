using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class AdditionalRequestMaterializerTest
{
	public AdditionalRequestMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new AdditionalRequestMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly AdditionalRequestMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void CreateRequestMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialQuery(datasource, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __additional_request
AS
SELECT
    r.sale_journals__req_i_sales_id,
    r.sale_id
FROM
    sale_journals__req_i_sales AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(datasource, requestMaterial);

		var expect = """
DELETE FROM
    sale_journals__req_i_sales AS d
WHERE
    (d.sale_journals__req_i_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__req_i_sales_id
        FROM
            (
                SELECT
                    t.sale_journals__req_i_sales_id,
                    t.sale_id
                FROM
                    __additional_request AS t
            ) AS r
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateCleanUpRequestMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

		var query = Proxy.CreateCleanUpRequestMaterialQuery(requestMaterial, datasource);

		var expect = """
DELETE FROM
    __additional_request AS d
WHERE
    (d.sale_journals__req_i_sales_id) IN (
        /* The data existing in KeyMap has been transformed, so delete it. */
        SELECT
            d.sale_journals__req_i_sales_id
        FROM
            (
                SELECT
                    t.sale_journals__req_i_sales_id,
                    t.sale_id
                FROM
                    __additional_request AS t
            ) AS d
            INNER JOIN sale_journals__key_m_sales AS keymap ON d.sale_id = keymap.sale_id
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}


}
