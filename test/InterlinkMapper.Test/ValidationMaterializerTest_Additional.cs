using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationMaterializerTest_Additional
{
	public ValidationMaterializerTest_Additional(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new AdditionalMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly AdditionaMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	public Material GetRequestMaterialFromValidation()
	{
		var validation = MaterialRepository.ValidationMaterial;
		return validation.ToAdditionalRequestMaterial();
	}

	[Fact]
	public void TestCreateMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = GetRequestMaterialFromValidation();

		var query = Proxy.CreateAdditionalMaterialQuery(datasource, requestMaterial);

		var expect = """
CREATE TEMPORARY TABLE
    __additional_datasource
AS
WITH
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
                    s.sale_date AS journal_closing_date,
                    s.sale_date,
                    s.shop_id,
                    s.price,
                    s.sale_id
                FROM
                    sales AS s
            ) AS d
            INNER JOIN __validation_datasource AS rm ON d.sale_id = rm.sale_id
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
