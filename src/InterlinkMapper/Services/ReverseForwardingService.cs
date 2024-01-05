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
		Materializer = new ReverseMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ReverseMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, InterlinkDestination destination)
	{
		Execute(connection, destination, null);
	}

	public void Execute(IDbConnection connection, InterlinkDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		var transaction = CreateTransactionAsNew(destination);
		connection.Save(transaction);

		var material = Materializer.Create(connection, transaction, injector);
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
