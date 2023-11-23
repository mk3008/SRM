using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Npgsql;
using RedOrb;

namespace AdditionalForwardingTest;

internal class Postgres(ILogger Logger) : IDbConnetionSetting
{
	public LoggingDbConnection ConnectionOpenAsNew()
	{
		var cnstring = "Server=localhost;Port=5430;Database=postgres;User Id=root;Password=root;";

		var cn = new NpgsqlConnection(cnstring);
		cn.Open();

		return new LoggingDbConnection(cn, Logger);
	}
}