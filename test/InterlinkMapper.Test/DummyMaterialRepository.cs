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
		var m = new AdditionalForwardingMaterializer(environment);
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
		var m = new ReverseForwardingMaterializer(environment);
		return m.AsPrivateProxy().CreateRequestMaterialQuery(DestinationRepository.sale_journals).ToSelectQuery();
	}

	public ValidationMaterial ValidationRequestMeterial =>
		new ValidationMaterial()
		{
			MaterialName = "__validation_request",
			Count = 1,
			SelectQuery = null!,
			CommandTimeout = 10,
			DatasourceKeyColumns = new(),
			DestinationColumns = null!,
			DestinationIdColumn = null!,
			DestinationTable = null!,
			KeymapTable = null!,
			OriginIdColumn = null!,
			PlaceHolderIdentifer = null!,
			ProcessIdColumn = null!,
			RelationTable = null!,
			RemarksColumn = null!,
			ReverseTable = null!,
			RootIdColumn = null!,
			KeymapTableNameColumn = null!,
		};

	public AdditionalMaterial AdditinalMeterial => CreateAdditionalMaterialQuery();

	public ReverseMaterial ReverseMeterial => CreateReverseMeterial();

	private AdditionalMaterial CreateAdditionalMaterialQuery()
	{
		var datasource = DatasourceRepository.sales;
		var request = AdditionalRequestMeterial;
		var service = new AdditionalForwardingMaterializer(environment);
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
		var service = new ReverseForwardingMaterializer(environment);
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
}
