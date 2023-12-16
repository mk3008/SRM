using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;

namespace InterlinkMapper.Test;

public class DummyMaterialRepository(SystemEnvironment environment)
{
	public Material AdditionalRequestMeterial =>
		new Material()
		{
			MaterialName = "__additional_request",
			Count = 1,
			SelectQuery = GetAdditionalRequestQuery(),
		};

	private SelectQuery GetAdditionalRequestQuery()
	{
		var m = new AdditionalMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DatasourceRepository.sales).ToSelectQuery();
	}

	public Material ReverseRequestMeterial =>
		new Material()
		{
			MaterialName = "__reverse_request",
			Count = 1,
			SelectQuery = GetReverseRequestQuery()
		};

	private SelectQuery GetReverseRequestQuery()
	{
		var m = new ReverseMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DestinationRepository.sale_journals).ToSelectQuery();
	}

	public Material ValidationRequestMeterial =>
		new Material()
		{
			MaterialName = "__validation_request",
			Count = 1,
			SelectQuery = GetValidationRequestQuery()
		};

	private SelectQuery GetValidationRequestQuery()
	{
		var m = new ValidationMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DatasourceRepository.sales).ToSelectQuery();
	}

	public AdditionalMaterial AdditinalMeterial => CreateAdditionalMaterialQuery();

	public ReverseMaterial ReverseMeterial => CreateReverseMeterial();

	public ValidationMaterial ValidationMaterial => CreateValidationMaterial();

	private AdditionalMaterial CreateAdditionalMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = AdditionalRequestMeterial;
		var service = new AdditionalMaterializer(environment);
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
		};

		return service.AsPrivateProxy().ToAdditionalMaterial(datasource, material);
	}

	private ReverseMaterial CreateReverseMeterial()
	{
		var destination = DestinationRepository.sale_journals;
		var request = ReverseRequestMeterial;
		var service = new ReverseMaterializer(environment);
		var query = service.AsPrivateProxy().CreateReverseMaterialQuery(
			destination,
			request,
			(SelectQuery x) => x
		);

		var material = new Material
		{
			Count = 1,
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
		};

		return service.AsPrivateProxy().ToReverseMaterial(destination, material);
	}

	private ValidationMaterial CreateValidationMaterial()
	{
		var datasource = DatasourceRepository.sales;
		var request = ValidationRequestMeterial;
		var service = new ValidationMaterializer(environment);
		var query = service.AsPrivateProxy().CreateValidationMaterialQuery(
			datasource,
			request,
			(SelectQuery x) => x
		);

		var material = new Material
		{
			Count = 1,
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
		};

		return service.AsPrivateProxy().ToValidationMaterial(datasource, material);
	}
}
