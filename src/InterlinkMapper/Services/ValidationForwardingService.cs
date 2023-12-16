using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;
using System.Diagnostics;

namespace InterlinkMapper.Services;

public class ValidationForwardingService
{
	public ValidationForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new ValidationMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ValidationMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.TransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var material = Materializer.Create(connection, datasource, injector);
		if (material == null || material.Count == 0) return;

		//transfer
		ExecuteReverse(connection, material, datasource.Destination, transaction.TransactionId);
		ExecuteAdditional(connection, material, datasource, transaction.TransactionId);
	}

	private void ExecuteReverse(IDbConnection connection, ValidationMaterial validation, DbDestination destination, long transactionId)
	{
		var request = validation.ToReverseRequestMaterial();
		var materializer = new ReverseForwardingMaterializer(Environment);
		var material = materializer.Create(connection, destination, request);
		material.ExecuteTransfer(connection, transactionId);
	}

	private void ExecuteAdditional(IDbConnection connection, ValidationMaterial validation, DbDatasource datasource, long transactionId)
	{
		var request = validation.ToAdditionalRequestMaterial();
		var materializer = new AdditionalForwardingMaterializer(Environment);
		var material = materializer.Create(connection, datasource, request);
		material.ExecuteTransfer(connection, transactionId);
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
			ActionName = nameof(ValidationForwardingService),
			TransactionId = transactionId,
			InsertCount = insertCount,
			KeyRelationTableName = keymap.Definition.TableFullName,
		};
		return row;
	}
}

[GeneratePrivateProxy(typeof(ValidationForwardingService))]
public partial struct ValidationForwardingServiceProxy;