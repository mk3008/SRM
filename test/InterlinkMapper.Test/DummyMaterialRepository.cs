using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;

namespace InterlinkMapper.Test;

public class DummyMaterialRepository(SystemEnvironment environment)
{
	public MaterializeResult AdditionalRequestMeterial =>
		new MaterializeResult()
		{
			MaterialName = "__additional_request",
		};

	public MaterializeResult ReverseRequestMeterial =>
		new MaterializeResult()
		{
			MaterialName = "__reverse_request",
		};

	public MaterializeResult AdditinalDatasourceMeterial => CreateAdditionalDatasourceMeterial();

	public MaterializeResult ReverseDatasourceMeterial => CreateReverseDatasourceMeterial();

	private MaterializeResult CreateAdditionalDatasourceMeterial()
	{
		var requestMaterial = AdditionalRequestMeterial;

		var service = new AdditionalForwardingMaterializer(environment);
		var query = service.AsPrivateProxy().CreateAdditionalDatasourceMaterialQuery(requestMaterial, DatasourceRepository.sales, (SelectQuery x) => x);

		return new MaterializeResult()
		{
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery()
		};
	}

	private MaterializeResult CreateReverseDatasourceMeterial()
	{
		var requestMaterial = ReverseRequestMeterial;

		var service = new ReverseForwardingMaterializer(environment);
		var query = service.AsPrivateProxy().CreateReverseDatasourceMaterialQuery(requestMaterial, DestinationRepository.sale_journals, (SelectQuery x) => x);

		return new MaterializeResult()
		{
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery()
		};
	}
}
