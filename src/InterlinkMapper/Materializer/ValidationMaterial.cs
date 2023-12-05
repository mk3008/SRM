namespace InterlinkMapper.Materializer;

public class ValidationMaterial : MaterializeResult
{
	public required string KeymapTable { get; init; }

	public required List<string> DatasourceKeyColumns { get; init; }

	public required string KeymapTableNameColumn { get; init; }

	public AdditionalMaterial ToAdditionalMaterial()
	{
		return new AdditionalMaterial
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = SelectQuery,
			DatasourceKeyColumns = DatasourceKeyColumns,
			RootIdColumn = RootIdColumn,
			OriginIdColumn = OriginIdColumn,
			RemarksColumn = RemarksColumn,
			DestinationTable = DestinationTable,
			DestinationColumns = DestinationColumns,
			DestinationIdColumn = DestinationIdColumn,
			KeymapTable = KeymapTable,
			PlaceHolderIdentifer = PlaceHolderIdentifer,
			CommandTimeout = CommandTimeout,
			ProcessIdColumn = ProcessIdColumn,
			RelationTable = RelationTable,
			ReverseTable = ReverseTable,
		};
	}

	public ReverseMaterial ToReverseMaterial()
	{
		return new ReverseMaterial
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = SelectQuery,
			RootIdColumn = RootIdColumn,
			OriginIdColumn = OriginIdColumn,
			RemarksColumn = RemarksColumn,
			DestinationTable = DestinationTable,
			DestinationColumns = DestinationColumns,
			DestinationIdColumn = DestinationIdColumn,
			KeymapTableNameColumn = KeymapTableNameColumn,
			PlaceHolderIdentifer = PlaceHolderIdentifer,
			CommandTimeout = CommandTimeout,
			ProcessIdColumn = ProcessIdColumn,
			RelationTable = RelationTable,
			ReverseTable = ReverseTable,
		};
	}
}
