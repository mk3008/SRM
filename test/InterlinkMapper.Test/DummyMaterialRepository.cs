using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using InterlinkMapper.Services;

namespace InterlinkMapper.Test;

public class DummyMaterialRepository(SystemEnvironment environment)
{

	private InterlinkDestination DummyDestination => DestinationRepository.sale_journals;

	private InterlinkTransaction DummyTransaction => SystemRepository.GetDummyTransaction(DummyDestination);

	public Material AdditionalRequestMeterial =>
		new Material()
		{
			MaterialName = "__additional_request",
			Count = 1,
			SelectQuery = GetAdditionalRequestQuery(),
			InterlinkTransaction = DummyTransaction
		};

	private SelectQuery GetAdditionalRequestQuery()
	{
		var m = new AdditionalRequestMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DatasourceRepository.sales, (SelectQuery x) => x).ToSelectQuery();
	}

	public Material ReverseRequestMeterial =>
		new Material()
		{
			MaterialName = "__reverse_request",
			Count = 1,
			SelectQuery = GetReverseRequestQuery(),
			InterlinkTransaction = DummyTransaction
		};

	private SelectQuery GetReverseRequestQuery()
	{
		var m = new ReverseRequestMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DestinationRepository.sale_journals, (SelectQuery x) => x).ToSelectQuery();
	}

	public Material ValidationRequestMeterial =>
		new Material()
		{
			MaterialName = "__validation_request",
			Count = 1,
			SelectQuery = GetValidationRequestQuery(),
			InterlinkTransaction = DummyTransaction
		};

	private SelectQuery GetValidationRequestQuery()
	{
		var m = new ValidationRequestMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DatasourceRepository.sales, (SelectQuery x) => x).ToSelectQuery();
	}

	public AdditionalMaterial AdditinalMeterial => CreateAdditionalMaterialQuery();

	public ReverseMaterial ReverseMeterial => CreateReverseMeterial();

	public DatasourceReverseMaterial DatasourceReverseMaterial => CreateDatasourceReverseMaterial();

	public ValidationMaterial ValidationMaterial => CreateValidationMaterial();

	private AdditionalMaterial CreateAdditionalMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = AdditionalRequestMeterial;
		var service = new AdditionalDatasourceMaterializer(environment);
		var query = service.AsPrivateProxy().CreateAdditionalMaterialQuery(
			datasource,
			request,
			(SelectQuery x) => x
		);

		var material = new Material
		{
			Count = 1,
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
			InterlinkTransaction = DummyTransaction
		};

		return service.AsPrivateProxy().ToAdditionalMaterial(datasource, material);
	}

	private ReverseMaterial CreateReverseMeterial()
	{
		var destination = DestinationRepository.sale_journals;
		var request = ReverseRequestMeterial;
		var service = new ReverseDatasourceMaterializer(environment);
		var query = service.AsPrivateProxy().CreateReverseMaterialQuery(
			destination,
			request
		);

		var material = new Material
		{
			Count = 1,
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
			InterlinkTransaction = DummyTransaction,
		};

		return service.AsPrivateProxy().ToReverseMaterial(material);
	}

	private DatasourceReverseMaterial CreateDatasourceReverseMaterial()
	{
		var reverse = CreateReverseMeterial();
		var m = reverse.AsPrivateProxy().ToDatasourceReverseMaterial(DatasourceRepository.sales);
		return m;
	}

	private ValidationMaterial CreateValidationMaterial()
	{
		var datasource = DatasourceRepository.sales;
		var request = ValidationRequestMeterial;
		var service = new ValidationDatasourceMaterializer(environment);
		var query = service.AsPrivateProxy().CreateValidationMaterialQuery(
			datasource,
			request
		);

		var material = new Material
		{
			Count = 1,
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
			InterlinkTransaction = DummyTransaction
		};

		return service.AsPrivateProxy().ToValidationMaterial(datasource, material);
	}
}
