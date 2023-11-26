using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ValidationMaterializerTest
{
	public ValidationMaterializerTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ValidationMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly ValidationMaterializerProxy Proxy;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void TestCreateRequestMaterialTableQuery()
	{
		var datasource = DatasourceRepository.sales;
		var query = Proxy.CreateRequestMaterialTableQuery(datasource);

		var expect = """
CREATE TEMPORARY TABLE
    __validation_request
AS
SELECT
    r.sale_journals__rv_sales_id,
    r.sale_id,
    r.created_at
FROM
    sale_journals__rv_sales AS r
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;
		var query = Proxy.CreateOriginDeleteQuery(requestMaterial, datasource);

		var expect = """
DELETE FROM
    sale_journals__rv_sales AS d
WHERE
    (d.sale_journals__rv_sales_id) IN (
        /* data that has been materialized will be deleted from the original. */
        SELECT
            r.sale_journals__rv_sales_id
        FROM
            sale_journals__rv_sales AS r
        WHERE
            EXISTS (
                SELECT
                    *
                FROM
                    __additional_request AS x
                WHERE
                    x.sale_journals__rv_sales_id = r.sale_journals__rv_sales_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCleanUpMaterialRequestQuery()
	{
		var datasource = DatasourceRepository.sales;
		var requestMaterial = MaterialRepository.ValidationRequestMeterial;
		var query = Proxy.CleanUpMaterialRequestQuery(requestMaterial, datasource);

		var expect = """
DELETE FROM
    __validation_request AS d
WHERE
    (d.sale_journal_id) IN (
        /* If it does not exist in the relation table, remove it from the target */
        SELECT
            r.sale_journal_id
        FROM
            __validation_request AS r
        WHERE
            NOT EXISTS (
                SELECT
                    *
                FROM
                    sale_journals__relation AS x
                WHERE
                    x.sale_journal_id = r.sale_journal_id
            )
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	//	[Fact]
	//	public void TestCreateDatasourceMaterialQuery()
	//	{
	//		var datasource = DatasourceRepository.sales;
	//		var requestMaterial = MaterialRepository.AdditionalRequestMeterial;

	//		var query = Proxy.CreateAdditionalDatasourceMaterialQuery(requestMaterial, datasource, (SelectQuery x) => x);

	//		var expect = """
	//CREATE TEMPORARY TABLE
	//    __datasource
	//AS
	//WITH
	//    _target_datasource AS (
	//        /* data source to be added */
	//        SELECT
	//            d.journal_closing_date,
	//            d.sale_date,
	//            d.shop_id,
	//            d.price,
	//            d.sale_id
	//        FROM
	//            (
	//                /* raw data source */
	//                SELECT
	//                    s.sale_date AS journal_closing_date,
	//                    s.sale_date,
	//                    s.shop_id,
	//                    s.price,
	//                    s.sale_id
	//                FROM
	//                    sales AS s
	//            ) AS d
	//        WHERE
	//            EXISTS (
	//                /* exists request material */
	//                SELECT
	//                    *
	//                FROM
	//                    __additional_request AS x
	//                WHERE
	//                    x.sale_id = d.sale_id
	//            )
	//    )
	//SELECT
	//    NEXTVAL('sale_journals_sale_journal_id_seq'::regclass) AS sale_journal_id,
	//    d.journal_closing_date,
	//    d.sale_date,
	//    d.shop_id,
	//    d.price,
	//    d.sale_id
	//FROM
	//    _target_datasource AS d
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}
}
