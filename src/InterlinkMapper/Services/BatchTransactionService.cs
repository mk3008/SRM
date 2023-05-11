using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchTransactionService
{
	public BatchTransactionService(DbEnvironment environment, IDbConnection connection, ILogger? logger = null)
	{
		Environment = environment;
		Connection = connection;
		Logger = logger;
	}

	private DbEnvironment Environment { get; init; }

	private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public int GetStart(IDatasource ds)
	{
		var env = Environment;

		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.DestinationTableNameColumn, ds.Destination.Table.GetTableFullName())).As(env.DestinationTableNameColumn);
		sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.DatasourceNameColumn, ds.DatasourceName)).As(env.DatasourceNameColumn);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(env.TransactionTable.GetTableFullName());
		iq.Returning(env.TransactionIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		var id = Connection.ExecuteScalar<int>(iq);

		Logger?.LogInformation($"TransactionId = {id}");
		return id;
	}
}
