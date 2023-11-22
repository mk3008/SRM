using Carbunql;

namespace InterlinkMapper.Batches;

public class FlipBridge
{
	public FlipBridge(IDestination destination, SelectQuery bridgeQuery)
	{
		Destination = destination;
		Query = bridgeQuery;
	}

	public IDestination Destination { get; init; }

	public List<string> KeymapTableNames { get; init; } = new();

	public List<string> RelationmapTableNames { get; init; } = new();

	public SelectQuery Query { get; init; }
}
