using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class SystemEnvironmentTest
{
	public SystemEnvironmentTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	private DbDatasource GetTestDatasouce()
	{
		return DatasourceRepository.sales;
	}

	//private MaterializeResult GetDummyRequestMeterial()
	//{
	//	return new MaterializeResult()
	//	{
	//		MaterialName = "__request",
	//		Count = 1,
	//		SelectQuery = null!
	//	};
	//}

	//private MaterializeResult GetDummyDatasourceMeterial()
	//{
	//	var requestMaterial = GetDummyRequestMeterial();

	//	var service = new AdditionalForwardingMaterializer(Environment);
	//	var query = service.AsPrivateProxy().CreateAdditionalDatasourceMaterialQuery(GetTestDatasouce(), requestMaterial, (SelectQuery x) => x);

	//	return new MaterializeResult()
	//	{
	//		MaterialName = query.TableFullName,
	//		SelectQuery = query.ToSelectQuery(),
	//		Count = 1,
	//	};
	//}

	private TransactionRow GetDummyTransactionRow()
	{
		return new TransactionRow()
		{
			DestinationId = 20,
			DatasourceId = 10,
			Argument = "argument"
		};
	}

	private ProcessRow GetDummyProcessRow()
	{
		var keymap = Environment.GetKeyMapTable(GetTestDatasouce());
		var keyrelation = Environment.GetKeyRelationTable(GetTestDatasouce());

		return new ProcessRow()
		{
			DestinationId = 20,
			DatasourceId = 10,
			ActionName = "test",
			TransactionId = 30,
			KeyMapTableName = keymap.Definition.TableName,
			KeyRelationTableName = keyrelation.Definition.TableName,
			InsertCount = 100
		};
	}

	[Fact]
	public void CreateTransactionInsertQuery()
	{
		var query = Environment.CreateTransactionInsertQuery(GetDummyTransactionRow());

		var expect = """
/*
  :destination_id = 20
  :datasource_id = 10
  :argument = 'argument'
*/
INSERT INTO
    interlink_transaction (
        destination_id, datasource_id, argument
    )
SELECT
    :destination_id AS destination_id,
    :datasource_id AS datasource_id,
    :argument AS argument
RETURNING
    interlink__transaction_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateProceeInsertQuery()
	{
		var query = Environment.CreateProcessInsertQuery(GetDummyProcessRow());

		var expect = """
/*
  :interlink__transaction_id = 30
  :datasource_id = 10
  :destination_id = 20
  :key_map = 'sale_journals__km_sales'
  :key_relation = 'sale_journals__kr_sales'
  :action = 'test'
  :insert_count = 100
*/
INSERT INTO
    interlink_process (
        interlink__transaction_id, datasource_id, destination_id, key_map, key_relation, action, insert_count
    )
SELECT
    :interlink__transaction_id AS interlink__transaction_id,
    :datasource_id AS datasource_id,
    :destination_id AS destination_id,
    :key_map AS key_map,
    :key_relation AS key_relation,
    :action AS action,
    :insert_count AS insert_count
RETURNING
    interlink__process_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	//	[Fact]
	//	public void CreateRelationInsertQuery()
	//	{
	//		var query = Environment.CreateRelationInsertQuery(GetTestDatasouce(), GetDummyDatasourceMeterial(), 40);

	//		var expect = """
	///*
	//  :interlink__process_id = 40
	//*/
	//INSERT INTO
	//    sale_journals__relation (
	//        sale_journal_id, interlink__process_id
	//    )
	//SELECT
	//    d.sale_journal_id,
	//    :interlink__process_id AS interlink__process_id
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.journal_closing_date,
	//            t.sale_date,
	//            t.shop_id,
	//            t.price,
	//            t.sale_id,
	//			t.root__sale_journal_id,
	//			t.origin__sale_journal_id
	//        FROM
	//            __datasource AS t
	//    ) AS d
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

	//	[Fact]
	//	public void CreateKeymapDeleteQuery()
	//	{
	//		var datasource = DatasourceRepository.sales;
	//		var datasourceMaterial = MaterialRepository.ReverseDatasourceMeterial;

	//		var query = Environment.CreateKeymapDeleteQuery(datasource, datasourceMaterial);

	//		var expect = """
	///* canceling the keymap due to reverse */
	//DELETE FROM
	//    sale_journals__m_sales AS d
	//WHERE
	//    (d.sale_journal_id) IN (
	//        SELECT
	//            d.origin__sale_journal_id AS sale_journal_id
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
	//                    t.keymap_name,
	//                    t.interlink__remarks
	//                FROM
	//                    __reverse_datasource AS t
	//            ) AS d
	//    )
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}
}
