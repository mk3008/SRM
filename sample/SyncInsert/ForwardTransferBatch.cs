﻿using Carbunql;
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

		Transfer(ds, bridge);

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
	private SelectQuery? PrepareForTransfer(IDatasource ds)
	{
		var service = new NotExistsBridgeService(Connection, Logger);
		var bridgeq = service.CreateAndSelect(ds);

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
	private void Transfer(IDatasource ds, SelectQuery bridge)
	{
		var service = new ForwardTransferService(Connection, Logger);

		service.TransferToDestination(ds, bridge);
		if (ds.HasRelationMapTable()) service.TransferToRelationMap(ds, bridge);
		if (ds.HasKeyMapTable()) service.TransferToKeyMap(ds, bridge);
		if (ds.HasRequestTable()) service.TransferToRequest(ds, bridge);
		if (ds.HasRequestTable()) service.RemoveRequestAsSuccess(ds, bridge);
	}
}
