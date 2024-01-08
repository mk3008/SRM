using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationRequestMaterializerTest
{
	public ValidationRequestMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ValidationRequestMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly ValidationRequestMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void CreateRequestMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialQuery(datasource, (SelectQuery x) => x);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_request
AS
SELECT
    r.sale_journals__req_v_sales_id,
    r.sale_id,
    keymap.sale_journal_id
FROM
    sale_journals__req_v_sales AS r
    LEFT JOIN sale_journals__key_m_sales AS keymap ON r.sale_id = keymap.sale_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(datasource, request);

		var expect = """
DELETE FROM
    sale_journals__req_v_sales AS d
WHERE
    (d.sale_journals__req_v_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__req_v_sales_id
        FROM
            (
                SELECT
                    t.sale_journals__req_v_sales_id,
                    t.sale_id,
                    t.sale_journal_id
                FROM
                    __validation_request AS t
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
		var request = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CreateCleanUpRequestMaterialQuery(request, datasource);

		var expect = """
DELETE FROM
    __validation_request AS d
WHERE
    (d.sale_journals__req_v_sales_id) IN (
        /* Data that does not exist in the KeyMap is not transferred and is not subject to verification. */
        SELECT
            d.sale_journals__req_v_sales_id
        FROM
            (
                SELECT
                    t.sale_journals__req_v_sales_id,
                    t.sale_id,
                    t.sale_journal_id
                FROM
                    __validation_request AS t
            ) AS d
		where
			d.sale_journal_id is null
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	//	[Fact]
	//	public void TestToAdditionalRequestMaterial()
	//	{
	//		var material = MaterialRepository.ValidationMaterial;
	//		var additional = material.ToAdditionalRequestMaterial();
	//		var query = additional.SelectQuery;

	//		var expect = """
	///* since the keymap is assumed to have been deleted in the reverses process, we will not check its existence here. */
	//SELECT
	//    d.sale_id,
	//    r.root__sale_journal_id,
	//    r.origin__sale_journal_id,
	//    d.interlink_remarks
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.sale_id,
	//            t.interlink_remarks
	//        FROM
	//            __validation_datasource AS t
	//    ) AS d
	//    INNER JOIN sale_journals__relation AS r ON d.sale_journal_id = r.origin__sale_journal_id
	//WHERE
	//    d.sale_id IS NOT null
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

	//	[Fact]
	//	public void TestToReverseRequestMaterial()
	//	{
	//		var material = MaterialRepository.ValidationMaterial;
	//		var reverse = material.ToReverseRequestMaterial();
	//		var query = reverse.SelectQuery;

	//		var expect = """
	//SELECT
	//    d.sale_journal_id,
	//    d.interlink_remarks
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.sale_id,
	//            t.interlink_remarks
	//        FROM
	//            __validation_datasource AS t
	//    ) AS d
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}
}
