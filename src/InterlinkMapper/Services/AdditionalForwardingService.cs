using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using RedOrb;
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
		if (datasource.Destination == null) throw new NullReferenceException(nameof(datasource.Destination));

		var transaction = CreateTransactionAsNew(datasource.Destination);
		connection.Save(transaction);

		var material = Materializer.Create(connection, transaction, datasource, injector);
		if (material == null || material.Count == 0) return;

		material.ExecuteTransfer(connection);
	}

	private InterlinkTransaction CreateTransactionAsNew(InterlinkDestination destination, string argument = "")
	{
		var tran = new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = nameof(AdditionalForwardingService),
			Argument = argument
		};
		return tran;
	}
}