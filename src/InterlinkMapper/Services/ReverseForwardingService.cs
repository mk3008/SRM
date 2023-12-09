using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using System.Data;

namespace InterlinkMapper.Services;

public class ReverseForwardingService
{
	public ReverseForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new ReverseForwardingMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ReverseForwardingMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.TransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var material = Materializer.Create(connection, datasource.Destination, injector);
		if (material == null || material.Count == 0) return;

		// create process row
		var process = CreateProcessRow(datasource, transaction.TransactionId, material.Count);
		process.ProcessId = connection.Execute(Environment.CreateProcessInsertQuery(process));

		material.ExecuteTransfer(connection, process.ProcessId);
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
		var keymap = Environment.GetKeyMapTable(datasource);
		var row = new ProcessRow()
		{
			ActionName = nameof(ReverseForwardingMaterializer),
			TransactionId = transactionId,
			InsertCount = insertCount,
			KeyRelationTableName = keymap.Definition.TableFullName,
		};
		return row;
	}
}
