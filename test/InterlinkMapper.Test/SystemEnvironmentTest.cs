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
  :interlink__destination_id = 20
  :interlink__datasource_id = 10
  :argument = 'argument'
*/
INSERT INTO
    interlink__transaction (
        interlink__destination_id, interlink__datasource_id, argument
    )
SELECT
    :interlink__destination_id AS interlink__destination_id,
    :interlink__datasource_id AS interlink__datasource_id,
    :argument AS argument
RETURNING
    interlink__transaction_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
