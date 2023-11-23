using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using RedOrb;
using Testcontainers.PostgreSql;

namespace AdditionalForwardingTest;

public class PostgresDB : IAsyncLifetime, IDbConnetionSetting
{
	private readonly PostgreSqlContainer Container = new PostgreSqlBuilder().WithImage("postgres:15-alpine").Build();

	public Task InitializeAsync()
	{
		return Container.StartAsync();
	}

	public Task DisposeAsync()
	{
		return Container.DisposeAsync().AsTask();
	}

	public ILogger? Logger { get; set; }

	public LoggingDbConnection ConnectionOpenAsNew(ILogger logger)
	{
		var cn = new NpgsqlConnection(Container.GetConnectionString());
		var lcn = new LoggingDbConnection(cn, logger);
		lcn.Open();
		return lcn;
	}

	public LoggingDbConnection ConnectionOpenAsNew()
	{
		if (Logger == null) throw new NullReferenceException(nameof(Logger));
		return ConnectionOpenAsNew(Logger);
	}
}