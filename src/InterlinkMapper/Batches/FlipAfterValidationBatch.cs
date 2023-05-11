using Carbunql;
using InterlinkMapper.Actions;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Batches;

/// <summary>
/// Transfer only data that has not been transferred.
/// </summary>
public class FlipAfterValidationBatch
{
	public FlipAfterValidationBatch(IDbConnectAction connector, ILogger? logger = null)
	{
		Connection = connector.Execute();
		Logger = logger;
	}

	private IDbConnection Connection { get; init; }

	public ILogger? Logger { get; init; }

	/// <summary>
	/// Execute the transfer process.
	/// </summary>
	/// <param name="ds"></param>
	public void Execute(IDatasource ds)
	{
		var destName = ds.Destination.Table.GetTableFullName();
		Logger!.LogInformation("start {Destination} <- {Datasource} <- {Destination}", destName, ds.DatasourceName, destName);

		using var trn = Connection.BeginTransaction();

		CreateEnvironmentOnDBMS(ds);

		var bridge = PrepareForTransfer(ds);
		if (bridge == null)
		{
			Logger?.LogWarning("Transfer target not found");
			return;
		}

		Transfer(bridge);

		trn.Commit();

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
		if (ds.HasForwardRequestTable()) service.CreateTableOrDefault(ds.ForwardRequestTable);
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <returns></returns>
	private Bridge? PrepareForTransfer(IDatasource ds)
	{
		var service = new NotExistsBridgeService(Connection, Logger);
		var bridgeQuery = service.CreateAndSelect(ds);

		var cnt = service.GetCount(bridgeQuery);
		if (cnt == 0) return null;
		return new Bridge(ds, bridgeQuery);
	}

	/// <summary>
	/// Transfers the contents of the bridge table to the map table and removes them from the request.
	/// Those that could not be transferred are transferred to the hold table.
	/// </summary>
	/// <param name="bridge"></param>
	private void Transfer(Bridge bridge)
	{
		//var service = new ForwardTransferService(0, Connection, Logger);
		//var ds = bridge.Datasource;

		//service.TransferToDestination(0, ds, bridge.Query);
		//if (ds.HasRelationMapTable()) service.TransferToRelationMap(ds, bridge.Query);
		//if (ds.HasKeyMapTable()) service.TransferToKeyMap(ds, bridge.Query);
		//if (ds.HasForwardRequestTable()) service.TransferToRequest(ds, bridge.Query);
		//if (ds.HasForwardRequestTable()) service.RemoveRequestAsSuccess(ds, bridge.Query);
	}

	private class Bridge
	{
		public Bridge(IDatasource datasource, SelectQuery bridgeQuery)
		{
			Datasource = datasource;
			Query = bridgeQuery;
		}

		public IDatasource Datasource { get; init; }
		public SelectQuery Query { get; init; }
	}
}
