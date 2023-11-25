using Dapper;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchTransactionService
{
	public BatchTransactionService(SystemEnvironment environment, ILogger? logger = null)
	{
		Environment = environment;
		Logger = logger;
	}

	private SystemEnvironment Environment { get; init; }

	private DbEnvironment DbEnvironment => Environment.DbEnvironment;

	///private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public TransactionRow Regist(IDbConnection connection, DbDatasource datasource)
	{
		return Regist(connection, datasource, string.Empty);
	}

	public TransactionRow Regist(IDbConnection connection, DbDatasource datasource, string argument)
	{
		var row = new TransactionRow()
		{
			DatasourceId = datasource.DatasourceId,
			DestinationId = datasource.Destination.DestinationId,
			Argument = argument
		};
		return Regist(connection, row);
	}

	private TransactionRow Regist(IDbConnection connection, TransactionRow row)
	{
		var table = Environment.GetTansactionTable();

		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.DestinationIdColumn, row.DestinationId);
		sq.Select(DbEnvironment, table.DatasourceIdColumn, row.DatasourceId);
		sq.Select(DbEnvironment, table.ArgumentColumn, row.Argument);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(table.Definition.TableFullName);
		iq.Returning(table.TransactionIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		row.TransactionId = connection.ExecuteScalar<long>(iq);

		Logger?.LogInformation($"TransactionId = {row.TransactionId}");
		return row;
	}

	private InsertQuery CreateTransactionInsertQuery(TransactionRow row)
	{
		var table = Environment.GetTansactionTable();

		//select :destination_name, :datasoruce_name
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.DestinationIdColumn, row.DestinationId);
		sq.Select(DbEnvironment, table.DatasourceIdColumn, row.DatasourceId);
		sq.Select(DbEnvironment, table.ArgumentColumn, row.Argument);

		//insert into transaction_table returning transaction_id
		var iq = sq.ToInsertQuery(table.Definition.TableFullName);
		iq.Returning(table.TransactionIdColumn);

		return iq;
	}
}
