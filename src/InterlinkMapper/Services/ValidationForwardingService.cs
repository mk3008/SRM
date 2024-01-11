using Carbunql.Building;
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
		DatasourceMaterializer = new ValidationDatasourceMaterializer(Environment);
		RequestMaterializer = new ValidationRequestMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ValidationDatasourceMaterializer DatasourceMaterializer { get; init; }

	private ValidationRequestMaterializer RequestMaterializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, InterlinkDatasource datasource)
	{
		Execute(connection, datasource, null);
	}

	public void Execute(IDbConnection connection, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		if (datasource.Destination == null) throw new NullReferenceException(nameof(datasource.Destination));

		var destination = datasource.Destination;

		var transaction = CreateTransactionAsNew(datasource.Destination);
		connection.Save(transaction);

		var request = RequestMaterializer.Create(connection, transaction, datasource, injector);
		if (request.Count == 0) return;

		var material = DatasourceMaterializer.Create(connection, transaction, datasource, request);
		if (material.Count == 0) return;

		ExecuteReverse(connection, transaction, datasource, material);
		ExecuteAdditional(connection, transaction, datasource, material);
	}

	private void ExecuteReverse(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, ValidationMaterial validation)
	{
		var requeset = validation.ToReverseDatasourceRequestMaterial();

		var materializer = new ReverseDatasourceMaterializer(Environment)
		{
			MaterialName = "__validation_reverse_datasource"
		};

		var material = materializer.Create(connection, transaction, requeset);
		if (material.Count == 0) return;

		material.ExecuteTransfer(connection);
	}

	private void ExecuteAdditional(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, ValidationMaterial validation)
	{
		var request = validation.ToAdditionalRequestMaterial();

		var materializer = new AdditionalDatasourceMaterializer(Environment)
		{
			MaterialName = "__validation_additional_datasource"
		};

		var material = materializer.Create(connection, transaction, datasource, request);
		if (material.Count == 0) return;

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