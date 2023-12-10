namespace InterlinkMapper.Materializer;

public class ValidationMaterial : MaterializeResult
{
	public required string KeymapTable { get; init; }

	//public required string KeyRelationTable { get; init; }

	//public required List<string> DatasourceKeyColumns { get; init; }

	public required string KeymapTableNameColumn { get; init; }

	public AdditionalMaterial ToAdditionalMaterial()
	{
		var sq = new SelectQuery();
		sq.AddComment("since the keymap is assumed to have been deleted in the reverses process, we will not check its existence here.");

		var (f, d) = sq.From(SelectQuery).As("d");
		var r = f.InnerJoin(ReverseTable).As("r").On(x =>
		{
			x.Condition(d, DestinationIdColumn).Equal(x.Table, OriginIdColumn);
		});

		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));
		sq.Select(r, RootIdColumn);
		sq.Select(r, OriginIdColumn);
		sq.Select(d, RemarksColumn);

		sq.Where(d, DatasourceKeyColumns.First()).IsNotNull();

		return new AdditionalMaterial
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = sq,
			DatasourceKeyColumns = DatasourceKeyColumns,
			RootIdColumn = RootIdColumn,
			OriginIdColumn = OriginIdColumn,
			RemarksColumn = RemarksColumn,
			DestinationTable = DestinationTable,
			DestinationColumns = DestinationColumns,
			DestinationIdColumn = DestinationIdColumn,
			KeyMapTable = KeymapTable,
			KeyRelationTable = KeyRelationTable,
			PlaceHolderIdentifer = PlaceHolderIdentifer,
			CommandTimeout = CommandTimeout,
			ProcessIdColumn = ProcessIdColumn,
			RelationTable = RelationTable,
			ReverseTable = ReverseTable,
			NumericType = string.Empty,
			ActionColumn = ActionColumn,
			ProcessDatasourceIdColumn = ProcessDatasourceIdColumn,
			ProcessDestinationIdColumn = ProcessDestinationIdColumn,
			InsertCountColumn = InsertCountColumn,
			KeyMapTableNameColumn = KeyMapTableNameColumn,
			KeyRelationTableNameColumn = KeyRelationTableNameColumn,
			ProcessTableName = ProcessTableName,
			TransactionIdColumn = TransactionIdColumn,
		};
	}

	public ReverseMaterial ToReverseMaterial()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, RemarksColumn);

		return new ReverseMaterial
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = sq,
			RootIdColumn = RootIdColumn,
			OriginIdColumn = OriginIdColumn,
			RemarksColumn = RemarksColumn,
			DestinationTable = DestinationTable,
			DestinationColumns = DestinationColumns,
			DestinationIdColumn = DestinationIdColumn,
			PlaceHolderIdentifer = PlaceHolderIdentifer,
			CommandTimeout = CommandTimeout,
			ProcessIdColumn = ProcessIdColumn,
			RelationTable = RelationTable,
			ReverseTable = ReverseTable,
			DatasourceKeyColumns = null!,
			KeyRelationTable = null!,

			ActionColumn = ActionColumn,
			ProcessDatasourceIdColumn = ProcessDatasourceIdColumn,
			ProcessDestinationIdColumn = ProcessDestinationIdColumn,
			InsertCountColumn = InsertCountColumn,
			KeyMapTableNameColumn = KeyMapTableNameColumn,
			KeyRelationTableNameColumn = KeyRelationTableNameColumn,
			ProcessTableName = ProcessTableName,
			TransactionIdColumn = TransactionIdColumn
		};
	}
}
