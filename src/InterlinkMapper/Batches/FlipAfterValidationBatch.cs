using Carbunql;
using InterlinkMapper.Services;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Batches;

/// <summary>
/// Transfer only data that has not been transferred.
/// </summary>
public class FlipAfterValidationBatch : ITransferBatch
{
	public FlipAfterValidationBatch(SystemEnvironment environment, ILogger? logger = null)
	{
		Environment = environment;
		Logger = logger;
	}

	public SystemEnvironment Environment { get; init; }

	public ILogger? Logger { get; init; }

	/// <summary>
	/// Execute the transfer process.
	/// </summary>
	/// <param name="ds"></param>
	public void Execute(IDatasource ds)
	{
		var destName = ds.Destination.Table.GetTableFullName();
		Logger!.LogInformation("start {Destination} <- {Datasource} <- {Destination}", destName, ds.DatasourceName, destName);

		using var cn = Environment.DbConnetionConfig.ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		var tranId = this.GetTranasctionId(cn, ds);
		var procId = this.GetProcessId(tranId, cn, ds);

		var bridge = CreateBridgeAsNew(cn, procId, ds);
		if (bridge == null)
		{
			trn.Commit();
			Logger?.LogWarning("Transfer target not found");
			return;
		}

		Transfer(cn, procId, bridge);

		trn.Commit();

		Logger!.LogInformation("end");
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <returns></returns>
	private Bridge? CreateBridgeAsNew(IDbConnection cn, int procId, IDatasource ds)
	{
		var service = new NotExistsBridgeService(Environment, cn, procId, Logger);
		var result = service.CreateAndSelectBridge(ds);

		if (result.Rows == 0) return null;
		return new Bridge(ds, result.SelectBridgeQuery);
	}

	/// <summary>
	/// Transfers the contents of the bridge table to the map table and removes them from the request.
	/// Those that could not be transferred are transferred to the hold table.
	/// </summary>
	/// <param name="bridge"></param>
	private void Transfer(IDbConnection cn, int procId, Bridge bridge)
	{
		var service = new ForwardTransferService(Environment, cn, procId, Logger);
		var ds = bridge.Datasource;

		service.TransferToDestination(ds, bridge.Query);
		if (ds.HasRelationMapTable()) service.TransferToRelationMap(ds, bridge.Query);
		if (ds.HasKeyMapTable()) service.TransferToKeyMap(ds, bridge.Query);
		if (ds.HasForwardRequestTable()) service.TransferToRequestAsHold(ds, bridge.Query);
		if (ds.HasForwardRequestTable()) service.DeleteRequestAsSuccess(ds, bridge.Query);
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
