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
			SelectQuery = null!,
		};

	public ReverseMaterial ReverseRequestMeterial =>
		new ReverseMaterial()
		{
			MaterialName = "__reverse_request",
			Count = 1,
			SelectQuery = null!,
			CommandTimeout = 10,
			DestinationColumns = null!,
			DestinationIdColumn = null!,
			DestinationTable = null!,
			OriginIdColumn = null!,
			PlaceHolderIdentifer = null!,
			ProcessIdColumn = null!,
			RelationTable = null!,
			RemarksColumn = null!,
			ReverseTable = null!,
			RootIdColumn = null!,
			KeymapTableNameColumn = null!,
		};

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

	public MaterializeResult AdditinalMeterial => CreateAdditionalMaterialQuery();

	public MaterializeResult ReverseDatasourceMeterial => CreateReverseDatasourceMeterial();

	private AdditionalMaterial CreateAdditionalMaterialQuery()
	{
		var requestMaterial = AdditionalRequestMeterial;

		var service = new AdditionalForwardingMaterializer(environment);
		var query = service.AsPrivateProxy().CreateAdditionalMaterialQuery(DatasourceRepository.sales, requestMaterial, (SelectQuery x) => x);

		return new AdditionalMaterial()
		{
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
			Count = 1,
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
		};
	}

	private ReverseMaterial CreateReverseDatasourceMeterial()
	{
		var requestMaterial = ReverseRequestMeterial;

		var service = new ReverseForwardingMaterializer(environment);
		var query = service.AsPrivateProxy().CreateReverseDatasourceMaterialQuery(DestinationRepository.sale_journals, requestMaterial, (SelectQuery x) => x);

		return new ReverseMaterial()
		{
			MaterialName = query.TableFullName,
			SelectQuery = query.ToSelectQuery(),
			Count = 1,
			CommandTimeout = 10,
			DestinationColumns = null!,
			DestinationIdColumn = null!,
			DestinationTable = null!,
			OriginIdColumn = null!,
			PlaceHolderIdentifer = null!,
			ProcessIdColumn = null!,
			RelationTable = null!,
			RemarksColumn = null!,
			ReverseTable = null!,
			RootIdColumn = null!,
			KeymapTableNameColumn = null!,
		};
	}
}
