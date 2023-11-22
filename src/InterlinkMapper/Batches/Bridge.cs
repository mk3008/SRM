namespace InterlinkMapper.Batches;

public class Bridge
{
	public Bridge(IDatasource datasource, SelectQuery bridgeQuery, SelectQuery requestQuery)
	{
		Datasource = datasource;
		DatasourceBridgeQuery = bridgeQuery;
		RequestQuery = requestQuery;
	}

	public IDatasource Datasource { get; init; }

	public SelectQuery DatasourceBridgeQuery { get; init; }

	public SelectQuery RequestQuery { get; init; }

	public SelectQuery ToSelectQueryAsSuccess()
	{
		var sq = new SelectQuery();
		var (_, bridge) = sq.From(DatasourceBridgeQuery).As("bridge");

		sq.Select(bridge);
		sq.Select(bridge, Datasource.Destination.Sequence.Column).As(Datasource.Destination.ProcessTable.RootIdColumnName);
		sq.Select("false").As(Datasource.Destination.ProcessTable.FlipFlagColumnName);

		sq.Where(bridge, Datasource.Destination.Sequence.Column).IsNotNull();

		return sq;
	}

	public SelectQuery ToSelectQueryAsHold()
	{
		var sq = new SelectQuery();
		var (_, bridge) = sq.From(DatasourceBridgeQuery).As("bridge");

		sq.Select(bridge);

		sq.Where(bridge, Datasource.Destination.Sequence.Column).IsNull();

		return sq;
	}

	public SelectQuery ToSelectRequestQueryAsNewHold()
	{
		var sq = this.ToSelectQueryAsHold();

		var d = sq.FromClause!.Root;

		// LEFT JOIN request AS req ON bridge.key1 = req.key1 AND bridge.key2 = req.key2
		// WHERE req.key1 IS NULL
		var req = sq.FromClause!.LeftJoin(Datasource.ForwardRequestTable.GetTableFullName()).As("req").On(d, Datasource.KeyColumns);
		sq.Where(req, Datasource.KeyColumns.First()).IsNull();

		return sq;
	}

	/// <summary>
	/// Select requests that have already been forwarded.
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="bridge"></param>
	/// <returns></returns>
	public SelectQuery ToSelectRequestQueryAsSuccess()
	{
		var requestId = Datasource.ForwardRequestTable.ColumnDefinitions.Where(x => x.IsAutoNumber).First();

		// SELECT r.id
		// FROM request AS r
		// INNER JOIN request_bridge AS rb ON r.id = rb.id
		// LEFT JOIN bridge AS b ON r.key1 = b.key1 AND r.key2 = b.key2
		var sq = new SelectQuery();
		var (f, r) = sq.From(Datasource.ForwardRequestTable.GetTableFullName()).As("r");
		f.InnerJoin(RequestQuery).As("rb").On(r, requestId.ColumnName);
		var b = f.LeftJoin(DatasourceBridgeQuery).As("b").On(r, Datasource.KeyColumns);

		// WHERE b.sequence IN NOT NULL
		sq.Where(b, Datasource.Destination.Sequence.Column).IsNotNull();

		sq.Select(r, requestId.ColumnName);

		return sq;
	}
}