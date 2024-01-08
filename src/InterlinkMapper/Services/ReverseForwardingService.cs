using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Services;

public class ReverseForwardingService
{
	public ReverseForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		RequestMaterializer = new ReverseRequestMaterializer(Environment);
		DatasourceMaterializer = new ReverseDatasourceMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ReverseRequestMaterializer RequestMaterializer { get; init; }

	private ReverseDatasourceMaterializer DatasourceMaterializer { get; init; }

	public void Execute(IDbConnection connection, InterlinkDestination destination)
	{
		Execute(connection, destination, null);
	}

	public void Execute(IDbConnection connection, InterlinkDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		var transaction = CreateTransactionAsNew(destination);
		connection.Save(transaction);

		var request = RequestMaterializer.Create(connection, transaction, injector);
		if (request.Count == 0) return;

		var material = DatasourceMaterializer.Create(connection, transaction, request);
		if (material == null || material.Count == 0) return;

		material.ExecuteTransfer(connection);
	}

	private InterlinkTransaction CreateTransactionAsNew(InterlinkDestination destination, string argument = "")
	{
		var tran = new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = nameof(ReverseForwardingService),
			Argument = argument
		};
		return tran;
	}
}
