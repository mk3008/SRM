using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;

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

	private DbTableConfig DbTableConfig => Environment.DbTableConfig;

	private DbQueryConfig DbQueryConfig => Environment.DbQueryConfig;


	private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public int GetStart(int transactionId, IDatasource ds)
	{
		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.TransactionIdColumn, transactionId);
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DestinationTableNameColumn, ds.Destination.Table.GetTableFullName());
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DatasourceNameColumn, ds.DatasourceName);

		if (ds.HasKeyMapTable())
		{
			sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.KeymapTableNameColumn, ds.KeyMapTable.GetTableFullName());
		}
		if (ds.HasRelationMapTable())
		{
			sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.RelationmapTableNameColumn, ds.RelationMapTable.GetTableFullName());
		}

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(DbTableConfig.ProcessTable.GetTableFullName());
		iq.Returning(DbTableConfig.ProcessIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		var id = Connection.ExecuteScalar<int>(iq);

		Logger?.LogInformation($"ProcessId = {id}");
		return id;
	}

	public int GetStart(int transactionId, IDestination ds, string datasrouce)
	{
		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.TransactionIdColumn, transactionId);
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DestinationTableNameColumn, ds.Table.GetTableFullName());
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, DbTableConfig.DatasourceNameColumn, datasrouce);

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(DbTableConfig.ProcessTable.GetTableFullName());
		iq.Returning(DbTableConfig.ProcessIdColumn);

		Logger?.LogInformation(iq.ToText() + ";");

		var id = Connection.ExecuteScalar<int>(iq);

		Logger?.LogInformation($"ProcessId = {id}");
		return id;
	}

	public void Finish(int processId)
	{
		//TODO：処理結果を記録する
	}
}
