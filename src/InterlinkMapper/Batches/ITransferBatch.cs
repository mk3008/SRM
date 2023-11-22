using InterlinkMapper.Models;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Batches;

public interface ITransferBatch
{
	SystemEnvironment Environment { get; init; }
}

public static class ITransferExtension
{
	public static int GetNewTranasctionId(this ITransferBatch source, LoggingDbConnection cn, IDestination ds)
	{
		var service = new BatchTransactionService(source.Environment, cn);
		return service.GetNewTransactionId(ds);
	}

	public static int GetNewTransactionId(this ITransferBatch source, LoggingDbConnection cn, IDestination ds, string datasource)
	{
		var service = new BatchTransactionService(source.Environment, cn);
		return service.GetNewTransactionId(ds, datasource);
	}

	public static int GetNewProcessId(this ITransferBatch source, int tranId, LoggingDbConnection cn, IDatasource ds)
	{
		var service = new BatchProcessService(source.Environment, cn);
		return service.PublishNewProcessId(tranId, ds);
	}
	public static int GetNewProcessId(this ITransferBatch source, int tranId, LoggingDbConnection cn, IDestination ds, string datasource)
	{
		var service = new BatchProcessService(source.Environment, cn);
		return service.PublishNewProcessId(tranId, ds, datasource);
	}
}