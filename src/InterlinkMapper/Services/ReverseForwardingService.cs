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
		Materializer = new ReverseMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ReverseMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.InterlinkTransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var material = Materializer.Create(connection, datasource.Destination, injector);
		if (material == null || material.Count == 0) return;

		material.ExecuteTransfer(connection, transaction.InterlinkTransactionId);
	}

	private InterlinkTransactionRow CreateTransactionRow(InterlinkDatasource datasource, string argument = "")
	{
		var row = new InterlinkTransactionRow()
		{
			InterlinkDestinationId = datasource.Destination.InterlinkDestinationId,
			InterlinkDatasourceId = datasource.InterlinkDatasourceId,
			Argument = argument
		};
		return row;
	}

	private InterlinkProcessRow CreateProcessRow(InterlinkDatasource datasource, long transactionId, int insertCount)
	{
		var keymap = Environment.GetKeyMapTable(datasource);
		var row = new InterlinkProcessRow()
		{
			ActionName = nameof(ReverseMaterializer),
			InterlinkTransactionId = transactionId,
			InsertCount = insertCount,
			KeyRelationTableName = keymap.Definition.TableFullName,
		};
		return row;
	}
}
