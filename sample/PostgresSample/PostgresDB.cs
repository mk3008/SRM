using Microsoft.Extensions.Logging;
using Npgsql;
using RedOrb;
using System.Data;

namespace PostgresSample;

public static class PostgresDB
{
	private static string ConnectionString = "Host=localhost;Port=5430;Username=root;Password=root;Database=postgres;";

	public static IDbConnection ConnectionOpenAsNew()
	{
		var cn = new NpgsqlConnection(ConnectionString);
		cn.Open();
		return cn;
	}

	public static IDbConnection ConnectionOpenAsNew(ILogger logger)
	{
		var cn = new NpgsqlConnection(ConnectionString);
		var lcn = new LoggingDbConnection(cn, logger);
		lcn.Open();
		return lcn;
	}
}
