using InterlinkMapper.Models;
using InterlinkMapper.Services;
using Microsoft.Extensions.Logging;

namespace InterlinkMapper.Batches;

public class DbEnvironmentBuildBatch
{
	public DbEnvironmentBuildBatch(SystemEnvironment environment, ILogger? logger = null)
	{
		Environment = environment;
		Logger = logger;
	}

	private SystemEnvironment Environment { get; init; }

	public ILogger? Logger { get; init; }

	public void Execute(IDatasource datasource)
	{
		using var cn = Environment.DbConnetionConfig.ConnectionOpenAsNew();

		var service = new DbEnvironmentService(cn, Logger);

		service.CreateTableOrDefault(Environment.DbTableConfig);
		service.CreateTableOrDefault(datasource.Destination);
		service.CreateTableOrDefault(datasource);
	}
}
