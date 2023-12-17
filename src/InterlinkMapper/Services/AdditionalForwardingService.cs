using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using System.Data;

namespace InterlinkMapper.Services;

public class AdditionalForwardingService
{
	public AdditionalForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new AdditionalMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private AdditionalMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, InterlinkDatasource datasource)
	{
		Execute(connection, datasource, null);
	}

	public void Execute(IDbConnection connection, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.InterlinkTransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var material = Materializer.Create(connection, datasource, injector);
		if (material == null || material.Count == 0) return;

		material.ExecuteTransfer(connection, transaction.InterlinkTransactionId);
	}

	private InterlinkTransactionRow CreateTransactionRow(InterlinkDatasource datasource, string argument = "")
	{
		var row = new InterlinkTransactionRow()
		{
			InterlinkDestinationId = datasource.Destination.InterlinkDestinationId,
			InterlinkDatasourceId = datasource.InterlinkDatasourceId,
			ActionName = nameof(AdditionalForwardingService),
			Argument = argument
		};
		return row;
	}

	private InterlinkProcessRow CreateProcessRow(InterlinkDatasource datasource, long transactionId, int insertCount)
	{
		var keymap = Environment.GetKeyMapTable(datasource);
		var relation = Environment.GetInterlinkRelationTable(datasource.Destination);
		var row = new InterlinkProcessRow()
		{
			ActionName = nameof(AdditionalForwardingService),
			InterlinkTransactionId = transactionId,
			InsertCount = insertCount,
			KeyMapTableName = keymap.Definition.TableFullName,
			KeyRelationTableName = relation.Definition.TableFullName,
		};
		return row;
	}
}