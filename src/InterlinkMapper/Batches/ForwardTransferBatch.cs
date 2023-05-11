using Carbunql;
using InterlinkMapper.Actions;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using System;
using System.Data;
using System.Data.Common;

namespace InterlinkMapper.Batches;

/// <summary>
/// Transfer only data that has not been transferred.
/// </summary>
public class ForwardTransferBatch
{
	public ForwardTransferBatch(DbEnvironment environment, IDbConnectAction connector, ILogger? logger = null)
	{
		Connector = connector;
		Logger = logger;
		Environment = environment;
	}

	private IDbConnectAction Connector { get; init; }

	public ILogger? Logger { get; init; }

	public DbEnvironment Environment { get; init; }

	/// <summary>
	/// Execute the transfer process.
	/// </summary>
	/// <param name="ds"></param>
	public void Execute(IDatasource ds)
	{
		Logger!.LogInformation("start {Destination} <- {Datasource}", ds.Destination.Table.GetTableFullName(), ds.DatasourceName);

		using var cn = Connector.Execute();
		using var trn = cn.BeginTransaction();

		var tranId = GetTranasctionId(cn, ds);
		var procId = GetProcessId(tranId, cn, ds);

		var bridge = PrepareForTransfer(cn, ds);
		if (bridge == null)
		{
			trn.Commit();
			Logger?.LogWarning("Transfer target not found");
			return;
		}

		Transfer(procId, cn, bridge);

		trn.Commit();

		Logger!.LogInformation("end");
	}

	private int GetTranasctionId(IDbConnection cn, IDatasource ds)
	{
		var service = new BatchTransactionService(Environment, cn, Logger);

		return service.GetStart(ds);
	}

	private int GetProcessId(int tranId, IDbConnection cn, IDatasource ds)
	{
		var service = new BatchProcessService(Environment, cn, Logger);

		return service.GetStart(tranId, ds);
	}

	/// <summary>
	/// Create a bridge table.
	/// </summary>
	/// <param name="ds"></param>
	/// <returns></returns>
	private Bridge? PrepareForTransfer(IDbConnection cn, IDatasource ds)
	{
		var service = new NotExistsBridgeService(cn, Logger);
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
	private void Transfer(int procId, IDbConnection cn, Bridge bridge)
	{
		var service = new ForwardTransferService(Environment, cn, Logger);
		var ds = bridge.Datasource;

		service.TransferToDestination(procId, ds, bridge.Query);
		if (ds.HasRelationMapTable()) service.TransferToRelationMap(procId, ds, bridge.Query);
		if (ds.HasKeyMapTable()) service.TransferToKeyMap(procId, ds, bridge.Query);
		if (ds.HasForwardRequestTable()) service.TransferToRequest(procId, ds, bridge.Query);
		if (ds.HasForwardRequestTable()) service.RemoveRequestAsSuccess(procId, ds, bridge.Query);
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
