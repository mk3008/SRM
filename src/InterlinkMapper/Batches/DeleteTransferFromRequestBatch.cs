using Carbunql;
using Carbunql.Building;
using InterlinkMapper.Services;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Batches;

public class DeleteTransferFromRequestBatch : ITransferBatch
{
	public DeleteTransferFromRequestBatch(SystemEnvironment environment, ILogger? logger = null)
	{
		Logger = logger;
		Environment = environment;
	}

	public ILogger? Logger { get; init; }

	public SystemEnvironment Environment { get; init; }

	public void Execute(IDestination ds)
	{
		Logger!.LogInformation("start {Destination} <- {Datasource}", ds.Table.GetTableFullName(), ds.DeleteRequestTable.GetTableFullName());

		using var cn = Environment.DbConnetionConfig.ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		var tranId = this.GetTranasctionId(cn, ds, ds.DeleteRequestTable.GetTableFullName());
		var procId = this.GetProcessId(tranId, cn, ds, ds.DeleteRequestTable.GetTableFullName());

		Transfer(ds, cn, procId);

		trn.Commit();

		Logger!.LogInformation("end");
	}

	private void Transfer(IDestination ds, IDbConnection cn, int processId)
	{
		var service = new DeleteRequestService(Environment, cn, processId, Logger);
		var maxRequestId = service.GetLastRequestId(ds);

		foreach (var item in service.GetKeymapTableNames(ds, maxRequestId))
		{
			service.DeleteKeymap(ds, maxRequestId, item);
		}
		foreach (var item in service.GetRelationmapTableNames(ds, maxRequestId))
		{
			service.DeleteRelationMap(ds, maxRequestId, item);
		}

		service.DeleteDestination(ds, maxRequestId);
		service.DeleteRequest(ds, maxRequestId);
	}
}
