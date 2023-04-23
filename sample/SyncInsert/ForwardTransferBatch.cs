using Carbunql;
using InterlinkMapper.Data;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using System.Data;

namespace SyncInsert;

/// <summary>
/// Transfer only data that has not been transferred.
/// </summary>
public class ForwardTransferBatch
{
	public ForwardTransferBatch(IDbConnection connection, ILogger? logger = null)
	{
		Connection = connection;
		Logger = logger;
	}

	private IDbConnection Connection { get; init; }

	public ILogger? Logger { get; init; }

	public string BridgeName { get; private set; } = string.Empty;

	/// <summary>
	/// Execute the transfer process.
	/// </summary>
	/// <param name="ds"></param>
	public void Execute(Datasource ds)
	{
		Logger!.LogInformation($"start {ds.Destination.Table.GetTableFullName()} <- {ds.DatasourceName}");

		CreateSystemTable(ds);

		var bridge = CreateBridgeTable(ds);
		if (bridge == null)
		{
			Logger?.LogInformation("Transfer target not found");
			return;
		}

		Transfer(ds, bridge);

		Logger!.LogInformation("end");
	}

	/// <summary>
	/// Generate the tables used by the system.
	/// </summary>
	/// <param name="ds"></param>
	private void CreateSystemTable(Datasource ds)
	{
		var service = new TableDefinitionService(Connection, Logger);

		if (ds.HasRelationMapTable) service.CreateOrDefault(ds.RelationMapTable);
		if (ds.HasKeyMapTable) service.CreateOrDefault(ds.KeyMapTable);
		if (ds.HasHoldTable) service.CreateOrDefault(ds.HoldTable);
		if (ds.HasRequestTable) service.CreateOrDefault(ds.RequestTable);
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <returns></returns>
	private SelectQuery? CreateBridgeTable(Datasource ds)
	{
		var service = new NotExistsBridgeService(Connection, Logger);
		if (string.IsNullOrEmpty(BridgeName)) BridgeName = service.GenerateBridgeName(ds);
		var bridgeq = service.CreateAsNew(ds, BridgeName);

		var cnt = service.GetCount(bridgeq);
		if (cnt == 0) return null;
		return bridgeq;
	}

	/// <summary>
	/// Transfers the contents of the bridge table to the map table and removes them from the request.
	/// Those that could not be transferred are transferred to the hold table.
	/// </summary>
	/// <param name="ds"></param>
	/// <param name="bridge"></param>
	private void Transfer(Datasource ds, SelectQuery bridge)
	{
		var service = new ForwardTransferService(Connection, Logger);

		service.TransferToDestination(ds, bridge);
		if (ds.HasRelationMapTable) service.TransferToRelationMap(ds, bridge);
		if (ds.HasKeyMapTable) service.TransferToKeyMap(ds, bridge);
		if (ds.HasHoldTable) service.TransferToHold(ds, bridge);
		if (ds.HasRequestTable) service.RemoveRequest(ds, bridge);
	}
}
