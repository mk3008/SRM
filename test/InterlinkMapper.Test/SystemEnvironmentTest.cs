using Carbunql;
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

	private InterlinkDatasource GetTestDatasouce()
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

	private InterlinkTransaction GetDummyTransactionRow(InterlinkDestination destination)
	{
		return new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = "test",
			Argument = "argument"
		};
	}

	private InterlinkProcess GetDummyProcessRow(InterlinkDatasource source)
	{
		return new InterlinkProcess()
		{
			InterlinkDatasource = source,
			ActionName = "test",
			InterlinkTransaction = GetDummyTransactionRow(source.Destination),
			InsertCount = 100
		};
	}

	//	[Fact]
	//	public void CreateTransactionInsertQuery()
	//	{
	//		var query = Environment.CreateTransactionInsertQuery(GetDummyTransactionRow());

	//		var expect = """
	///*
	//  :interlink_destination_id = 20
	//  :service_name = 'test'
	//  :argument = 'argument'
	//*/
	//INSERT INTO
	//    interlink_transaction (
	//        interlink_destination_id, service_name, argument
	//    )
	//SELECT
	//    :interlink_destination_id AS interlink_destination_id,
	//    :service_name AS service_name,
	//    :argument AS argument
	//RETURNING
	//    interlink_transaction_id
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}
}
