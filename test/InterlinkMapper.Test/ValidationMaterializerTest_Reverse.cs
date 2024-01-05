using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationMaterializerTest_Reverse
{
	public ValidationMaterializerTest_Reverse(ITestOutputHelper output)
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

	public readonly ReverseMaterializerProxy Proxy;

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

		var query = Proxy.CreateReverseMaterialQuery(datasource.Destination, requestMaterial);

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
            INNER JOIN __validation_datasource AS rm ON d.sale_journal_id = rm.sale_journal_id
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
    _target_datasource AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

}
