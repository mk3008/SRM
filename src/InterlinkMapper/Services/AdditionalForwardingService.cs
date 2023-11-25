using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Services;

public class AdditionalForwardingService
{
	public AdditionalForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new AdditionalForwardingMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private AdditionalForwardingMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var transaction = CreateTransactionRow(datasource);
		transaction.TransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var datasourceMaterial = Materializer.Create(connection, datasource, injector);
		if (datasourceMaterial == null || datasourceMaterial.Count == 0) return;

		var process = CreateProcessRow(datasource, transaction.TransactionId, datasourceMaterial.Count);
		process.ProcessId = connection.Execute(Environment.CreateProceeInsertQuery(process));

		connection.Execute(datasource.Destination.CreateInsertQueryFrom(datasourceMaterial), commandTimeout: CommandTimeout);
		connection.Execute(Environment.CreateKeymapInsertQuery(datasource, datasourceMaterial), commandTimeout: CommandTimeout);
		connection.Execute(Environment.CreateRelationInsertQuery(datasource, datasourceMaterial, process.ProcessId), commandTimeout: CommandTimeout);
	}

	private TransactionRow CreateTransactionRow(DbDatasource datasource, string argument = "")
	{
		var row = new TransactionRow()
		{
			DestinationId = datasource.Destination.DestinationId,
			DatasourceId = datasource.DatasourceId,
			Argument = argument
		};
		return row;
	}

	private ProcessRow CreateProcessRow(DbDatasource datasource, long transactionId, int insertCount)
	{
		var keymap = Environment.GetKeymapTable(datasource);
		var row = new ProcessRow()
		{
			ActionName = nameof(AdditionalForwardingService),
			TransactionId = transactionId,
			InsertCount = insertCount,
			KeymapTableName = keymap.Definition.TableFullName,
		};
		return row;
	}

	//private TransactionRow GetTransaction(IDbConnection cn, DbDatasource datasource)
	//{
	//	var service = new BatchTransactionService(Environment, cn);
	//	return service.Regist(datasource);
	//}

	//private ProcessRow GetProcess(IDbConnection cn, TransactionRow transaction, DbDatasource datasource, int rows)
	//{
	//	var service = new BatchProcessService(Environment, cn);
	//	return service.Regist(transaction, datasource, nameof(AdditionalForwardingService), rows);
	//}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingService))]
public partial struct AdditionalForwardingServiceProxy;