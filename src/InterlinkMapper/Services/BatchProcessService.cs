using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using System.Data;
using Dapper;

namespace InterlinkMapper.Services;

public class BatchProcessService
{
	public BatchProcessService(SystemEnvironment environment, IDbConnection connection, ILogger? logger = null)
	{
		Environment = environment;
		Connection = connection;
		Logger = logger;
	}

	private SystemEnvironment Environment { get; init; }

	private DbEnvironment DbEnvironment => Environment.DbEnvironment;

	private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public ProcessRow Regist(TransactionRow transaction, DbDatasource datasource, string actionName, int insertCount)
	{
		var keymap = Environment.GetKeymapTable(datasource);

		var row = new ProcessRow()
		{
			TransactionId = transaction.TransactionId,
			DatasourceId = datasource.DatasourceId,
			DestinationId = datasource.Destination.DestinationId,
			KeymapTableName = keymap.Definition.TableFullName,
			ActionName = actionName,
			InsertCount = insertCount
		};
		return Regist(row);
	}

	private ProcessRow Regist(ProcessRow row)
	{
		var table = Environment.GetProcessTable();

		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(DbEnvironment, table.TransactionIdColumn, row.TransactionId);
		sq.Select(DbEnvironment, table.DatasourceIdColumn, row.DatasourceId);
		sq.Select(DbEnvironment, table.DestinationIdColumn, row.DestinationId);
		sq.Select(DbEnvironment, table.KeymapTableNameColumn, row.KeymapTableName);
		sq.Select(DbEnvironment, table.ActionColumn, row.ActionName);
		sq.Select(DbEnvironment, table.InsertCountColumn, row.InsertCount);

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(table.Definition.GetTableFullName());
		iq.Returning(table.ProcessIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		row.ProcessId = Connection.ExecuteScalar<int>(iq);

		Logger?.LogInformation($"ProcessId = {row.ProcessId}");
		return row;
	}
}
