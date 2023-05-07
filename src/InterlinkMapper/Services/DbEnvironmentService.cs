using Carbunql.Analysis.Parser;
using Carbunql.Extensions;
using Cysharp.Text;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class DbEnvironmentService
{
	public DbEnvironmentService(IDbConnection cn, ILogger? logger = null)
	{
		Connection = cn;
		Logger = logger;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }

	public int CommandTimeout { get; set; } = 60 * 15;

	public void CreateTableOrDefault(DbTableDefinition def)
	{
		var sql = def.ToCreateCommandText();
		Logger?.LogInformation("create table sql : {Sql}", sql);

		Connection.Execute(sql, CommandTimeout);
	}
}
