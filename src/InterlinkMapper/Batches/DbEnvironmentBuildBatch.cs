using InterlinkMapper.Actions;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;

namespace InterlinkMapper.Batches;

public class DbEnvironmentBuildBatch
{
	public DbEnvironmentBuildBatch(IDbConnectAction connector, ILogger? logger = null)
	{
		Connector = connector;
		Logger = logger;
	}

	private IDbConnectAction Connector { get; init; }

	public ILogger? Logger { get; init; }

	public void Execute(DbEnvironment environment, IDatasource datasource)
	{
		using var cn = Connector.Execute();

		var service = new DbEnvironmentService(cn, Logger);

		service.CreateTableOrDefault(environment);
		service.CreateTableOrDefault(datasource.Destination);
		service.CreateTableOrDefault(datasource);
	}
}
