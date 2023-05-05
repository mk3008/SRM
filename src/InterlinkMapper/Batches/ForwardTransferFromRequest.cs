using Carbunql;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Batches;

/// <summary>
/// Transfer only data that has not been transferred.
/// </summary>
public class ForwardTransferFromRequest
{
	public ForwardTransferFromRequest(IDbConnection connection, ILogger? logger = null)
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
	public void Execute(IDatasource ds)
	{
		Logger!.LogInformation("start {Destination} <- {Datasource}", ds.Destination.Table.GetTableFullName(), ds.DatasourceName);

		CreateEnvironmentOnDBMS(ds);

		var bridge = PrepareForTransfer(ds);
		if (bridge == null)
		{
			Logger?.LogWarning("Transfer target not found");
			return;
		}

		Transfer(bridge);

		Logger!.LogInformation("end");
	}

	/// <summary>
	/// Generate the tables used by the system.
	/// </summary>
	/// <param name="ds"></param>
	private void CreateEnvironmentOnDBMS(IDatasource ds)
	{
		var service = new DbEnvironmentService(Connection, Logger);

		if (ds.HasRelationMapTable()) service.CreateTableOrDefault(ds.RelationMapTable);
		if (ds.HasKeyMapTable()) service.CreateTableOrDefault(ds.KeyMapTable);
		if (ds.HasRequestTable()) service.CreateTableOrDefault(ds.RequestTable);
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <returns></returns>
	private Bridge? PrepareForTransfer(IDatasource ds)
	{
		var service = new RequestBridgeService(Connection, Logger);
		var maxid = service.GetLastRequestId(ds);
		var bridgeQuery = service.CreateAndSelect(ds, maxid);

		var cnt = service.GetCount(bridgeQuery);
		if (cnt == 0) return null;

		var bridge = new Bridge(ds, bridgeQuery, cnt);
		return bridge;
	}

	/// <summary>
	/// Transfers the contents of the bridge table to the map table and removes them from the request.
	/// Those that could not be transferred are transferred to the hold table.
	/// </summary>
	/// <param name="bridge"></param>
	private void Transfer(Bridge bridge)
	{
		var service = new ForwardTransferService(Connection, Logger);

		var ds = bridge.Datasource;
		service.TransferToDestination(ds, bridge.Query);
		if (ds.HasRelationMapTable()) service.TransferToRelationMap(ds, bridge.Query);
		if (ds.HasKeyMapTable()) service.TransferToKeyMap(ds, bridge.Query);
		if (ds.HasRequestTable()) service.RemoveRequestAsSuccess(ds, bridge.Query);
		if (ds.HasRequestTable()) service.RemoveRequestAsIgnore(ds, bridge.Query, bridge.MaxRequestId);
	}

	private class Bridge
	{
		public Bridge(IDatasource datasource, SelectQuery bridgeQuery, int maxRequestId)
		{
			Datasource = datasource;
			Query = bridgeQuery;
			MaxRequestId = maxRequestId;
		}

		public IDatasource Datasource { get; init; }
		public SelectQuery Query { get; init; }
		public int MaxRequestId { get; init; }
	}
}
