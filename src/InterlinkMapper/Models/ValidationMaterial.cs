using InterlinkMapper.Materializer;

namespace InterlinkMapper.Models;

public class ValidationMaterial : DatasourceMaterial
{
	public required List<string> DatasourceKeyColumns { get; set; }

	public required string KeyMapTableFullName { get; set; }

	public required string KeyRelationTableFullName { get; set; }

	public required int Count { get; set; }

	public Material ToReverseDatasourceRequestMaterial()
	{
		return new Material
		{
			Count = -1,
			MaterialName = MaterialName,
			SelectQuery = SelectQuery,
			InterlinkTransaction = InterlinkTransaction,
		};
	}

	public Material ToAdditionalRequestMaterial()
	{
		return new Material
		{
			Count = -1,
			MaterialName = MaterialName,
			SelectQuery = SelectQuery,
			InterlinkTransaction = InterlinkTransaction,
		};
	}
}
