namespace InterlinkMapper.Materializer;

public class ValidationMaterial : MaterializeResult
{
	public required string KeymapTable { get; init; }

	public required string KeymapTableNameColumn { get; init; }

	public Material ToAdditionalRequestMaterial()
	{
		var sq = new SelectQuery();
		sq.AddComment("since the keymap is assumed to have been deleted in the reverses process, we will not check its existence here.");

		var (f, d) = sq.From(SelectQuery).As("d");
		var r = f.InnerJoin(InterlinkRelationTable).As("r").On(x =>
		{
			x.Condition(d, DestinationIdColumn).Equal(x.Table, OriginIdColumn);
		});

		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));
		sq.Select(r, RootIdColumn);
		sq.Select(r, OriginIdColumn);
		sq.Select(d, InterlinkRemarksColumn);

		sq.Where(d, DatasourceKeyColumns.First()).IsNotNull();

		return new Material
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = sq,
		};
	}

	public Material ToReverseRequestMaterial()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		sq.Select(d, DestinationIdColumn);
		sq.Select(d, InterlinkRemarksColumn);

		return new Material
		{
			Count = Count,
			MaterialName = MaterialName,
			SelectQuery = sq,
		};
	}
}
