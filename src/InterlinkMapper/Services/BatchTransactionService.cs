using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchTransactionService
{
	public BatchTransactionService(SystemEnvironment environment, IDbConnection connection, ILogger? logger = null)
	{
		Environment = environment;
		Connection = connection;
		Logger = logger;
	}

	private SystemEnvironment Environment { get; init; }

	private DbTableConfig DbTableConfig => Environment.DbTableConfig;

	private DbQueryConfig DbQueryConfig => Environment.DbQueryConfig;

	private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public int GetStart(IDatasource ds)
	{
		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DestinationTableNameColumn, ds.Destination.Table.GetTableFullName());
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DatasourceNameColumn, ds.DatasourceName);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(DbTableConfig.TransactionTable.GetTableFullName());
		iq.Returning(DbTableConfig.TransactionIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		var id = Connection.ExecuteScalar<int>(iq);

		Logger?.LogInformation($"TransactionId = {id}");
		return id;
	}
}
