using InterlinkMapper.Models;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using RedOrb;

namespace InterlinkMapper.Batches;

public class FlipTransferBatch : ITransferBatch
{
	public FlipTransferBatch(SystemEnvironment environment)
	{
		Environment = environment;
	}

	public SystemEnvironment Environment { get; init; }

	public void Execute(IDestination destination)
	{
		using var cn = Environment.DbConnetionConfig.ConnectionOpenAsNew();

		cn.LogInformation("start {Destination} <- {Datasource}", destination.Table.GetTableFullName(), destination.ReversalOption.RequestTable.GetTableFullName());

		using var trn = cn.BeginTransaction();

		var tranId = this.GetNewTransactionId(cn, destination, destination.DeleteRequestTable.GetTableFullName());
		var procId = this.GetNewProcessId(tranId, cn, destination, destination.DeleteRequestTable.GetTableFullName());

		var bridge = CreateBridgeAsNew(cn, procId, destination);
		if (bridge == null)
		{
			trn.Commit();
			Logger?.LogWarning("Flip requests not found");
			return;
		}

		Transfer(cn, procId, bridge);

		trn.Commit();

		Logger!.LogInformation("end");
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="destination"></param>
	/// <returns></returns>
	private FlipBridge? CreateBridgeAsNew(LoggingDbConnection cn, int processId, IDestination destination)
	{
		var service = new FlipRequestBridgeService(Environment, cn, processId, Logger);
		var (sq, result) = service.CreateAndSelectBridge(destination);

		if (result == 0) return null;

		service.DeleteRequestFromBridge(destination, sq);

		var bridge = new FlipBridge(destination, sq);

		foreach (var keymapTableName in service.GetKeymapTableNamesFromBridge(destination, sq))
		{
			bridge.KeymapTableNames.Add(keymapTableName);
		}
		foreach (var relationTableName in service.GetRelatinmapTableNamesFromBridge(destination, sq))
		{
			bridge.RelationmapTables.Add(relationTableName);
		}

		return bridge;
	}

	/// <summary>
	/// Transfers the contents of the bridge table to the map table and removes them from the request.
	/// Those that could not be transferred are transferred to the hold table.
	/// </summary>
	/// <param name="bridge"></param>
	private void Transfer(LoggingDbConnection cn, int processId, FlipBridge bridge)
	{
		var service = new FlipTransferService(Environment, cn, processId);

		service.TransferToDestinationFromBridge(bridge.Destination, bridge.Query);
		service.TransferToRelationMap(bridge.Destination, bridge.Query);
	}

	//private class Bridge
	//{
	//	public Bridge(IDestination destination, SelectQuery bridgeQuery)
	//	{
	//		Destination = destination;
	//		Query = bridgeQuery;
	//	}

	//	public IDestination Destination { get; init; }

	//	public List<string> KeymapTableNames { get; init; } = new();

	//	public List<string> RelationmapTableNames { get; init; } = new();

	//	public SelectQuery Query { get; init; }
	//}
}
