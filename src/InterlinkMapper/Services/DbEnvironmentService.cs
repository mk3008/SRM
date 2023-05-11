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
		Logger?.LogInformation(sql + ";");
		Connection.Execute(sql, CommandTimeout);

		foreach (var index in def.Indexes)
		{
			var q = index.ToCreateCommandText(def);
			Logger?.LogInformation(q + ";");
			Connection.Execute(q, CommandTimeout);
		}
	}

	public void CreateTableOrDefault(DbEnvironment environment)
	{
		CreateTableOrDefault(environment.TransactionTable);
		CreateTableOrDefault(environment.ProcessTable);
		CreateTableOrDefault(environment.ProcessResultTable);
	}

	public void CreateTableOrDefault(IDatasource d)
	{
		if (d.HasRelationMapTable()) CreateTableOrDefault(d.RelationMapTable);
		if (d.HasKeyMapTable()) CreateTableOrDefault(d.KeyMapTable);
		if (d.HasForwardRequestTable()) CreateTableOrDefault(d.ForwardRequestTable);
		if (d.HasValidateRequestTable()) CreateTableOrDefault(d.ValidateRequestTable);
	}

	public void CreateTableOrDefault(IDestination d)
	{
		if (d.HasProcessTable()) CreateTableOrDefault(d.ProcessTable);
		if (d.HasFlipTable()) CreateTableOrDefault(d.FlipOption.FlipTable);
		if (d.HasValidateRequestTable()) CreateTableOrDefault(d.ValidateRequestTable);
	}
}
