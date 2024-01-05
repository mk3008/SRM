using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

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
		var destination = datasource.Destination;

		var transaction = CreateTransactionAsNew(destination);
		connection.Save(transaction);

		var material = Materializer.Create(connection, transaction, datasource, injector);
		if (material == null) return;

		//transfer
		ExecuteReverse(connection, transaction, material);
		ExecuteAdditional(connection, transaction, datasource, material);
	}

	private void ExecuteReverse(IDbConnection connection, InterlinkTransaction transaction, ValidationMaterial validation)
	{
		var request = validation.ToReverseRequestMaterial();

		var materializer = new ReverseMaterializer(Environment);
		var material = materializer.Create(connection, transaction, request);

		material.ExecuteTransfer(connection);
	}

	private void ExecuteAdditional(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, ValidationMaterial validation)
	{
		var request = validation.ToAdditionalRequestMaterial();

		var materializer = new AdditionalMaterializer(Environment);
		var material = materializer.Create(connection, transaction, datasource, request);

		material.ExecuteTransfer(connection);
	}

	private InterlinkTransaction CreateTransactionAsNew(InterlinkDestination destination, string argument = "")
	{
		var tran = new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = nameof(ValidationForwardingService),
			Argument = argument
		};
		return tran;
	}
}

[GeneratePrivateProxy(typeof(ValidationForwardingService))]
public partial struct ValidationForwardingServiceProxy;