using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class AdditinalMaterialTest
{

	public AdditinalMaterialTest(ITestOutputHelper output)
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

	//private DbDatasource GetTestDatasouce()
	//{
	//	return DatasourceRepository.sales;
	//}

	//private MaterializeResult GetDummyRequestMeterial()
	//{
	//	return new MaterializeResult()
	//	{
	//		MaterialName = "__additional_request",
	//	};
	//}

	[Fact]
	public void TestCreateRequestMaterialTableQuery()
	{

	}
}
