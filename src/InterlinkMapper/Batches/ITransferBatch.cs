using InterlinkMapper.Services;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Batches;

public interface ITransferBatch
{
	ILogger? Logger { get; init; }

	SystemEnvironment Environment { get; init; }
}

public static class ITransferExtension
{
	public static int GetTranasctionId(this ITransferBatch source, IDbConnection cn, IDatasource ds)
	{
		var service = new BatchTransactionService(source.Environment, cn, source.Logger);
		return service.GetStart(ds);
	}

	public static int GetTranasctionId(this ITransferBatch source, IDbConnection cn, IDestination ds, string datasource)
	{
		var service = new BatchTransactionService(source.Environment, cn, source.Logger);
		return service.GetStart(ds, datasource);
	}

	public static int GetProcessId(this ITransferBatch source, int tranId, IDbConnection cn, IDatasource ds)
	{
		var service = new BatchProcessService(source.Environment, cn, source.Logger);
		return service.GetStart(tranId, ds);
	}
	public static int GetProcessId(this ITransferBatch source, int tranId, IDbConnection cn, IDestination ds, string datasource)
	{
		var service = new BatchProcessService(source.Environment, cn, source.Logger);
		return service.GetStart(tranId, ds, datasource);
	}
}