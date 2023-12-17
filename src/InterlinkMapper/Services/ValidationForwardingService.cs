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

	public void Execute(IDbConnection connection, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.InterlinkTransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var material = Materializer.Create(connection, datasource, injector);
		if (material == null || material.Count == 0) return;

		//transfer
		ExecuteReverse(connection, material, datasource.Destination, transaction.InterlinkTransactionId);
		ExecuteAdditional(connection, material, datasource, transaction.InterlinkTransactionId);
	}

	private void ExecuteReverse(IDbConnection connection, ValidationMaterial validation, InterlinkDestination destination, long transactionId)
	{
		var request = validation.ToReverseRequestMaterial();
		var materializer = new ReverseMaterializer(Environment);
		var material = materializer.Create(connection, destination, request);
		material.ExecuteTransfer(connection, transactionId);
	}

	private void ExecuteAdditional(IDbConnection connection, ValidationMaterial validation, InterlinkDatasource datasource, long transactionId)
	{
		var request = validation.ToAdditionalRequestMaterial();
		var materializer = new AdditionalMaterializer(Environment);
		var material = materializer.Create(connection, datasource, request);
		material.ExecuteTransfer(connection, transactionId);
	}

	private InterlinkTransactionRow CreateTransactionRow(InterlinkDatasource datasource, string argument = "")
	{
		var row = new InterlinkTransactionRow()
		{
			InterlinkDestinationId = datasource.Destination.InterlinkDestinationId,
			InterlinkDatasourceId = datasource.InterlinkDatasourceId,
			ServiceName = nameof(ValidationForwardingService),
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
			ActionName = nameof(ValidationForwardingService),
			InterlinkTransactionId = transactionId,
			InsertCount = insertCount,
			KeyMapTableName = keymap.Definition.TableFullName,
			KeyRelationTableName = relation.Definition.TableFullName,
		};
		return row;
	}
}

[GeneratePrivateProxy(typeof(ValidationForwardingService))]
public partial struct ValidationForwardingServiceProxy;