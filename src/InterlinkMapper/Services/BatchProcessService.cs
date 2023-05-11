using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using Microsoft.Extensions.Logging;
using System;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchProcessService
{
	public BatchProcessService(DbEnvironment environment, IDbConnection connection, ILogger? logger = null)
	{
		Environment = environment;
		Connection = connection;
		Logger = logger;
	}

	private DbEnvironment Environment { get; init; }

	private IDbConnection Connection { get; init; }

	private readonly ILogger? Logger;

	public int GetStart(int transactionId, IDatasource ds)
	{
		var env = Environment;

		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.TransactionIdColumn, transactionId)).As(env.TransactionIdColumn);
		sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.DestinationTableNameColumn, ds.Destination.Table.GetTableFullName())).As(env.DestinationTableNameColumn);
		sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.DatasourceNameColumn, ds.DatasourceName)).As(env.DatasourceNameColumn);

		if (ds.HasKeyMapTable())
		{
			sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.KeymapTableNameColumn, ds.KeyMapTable.GetTableFullName())).As(env.KeymapTableNameColumn);
		}
		if (ds.HasRelationMapTable())
		{
			sq.Select(sq.AddParameter(env.PlaceHolderIdentifer + env.RelationmapTableNameColumn, ds.RelationMapTable.GetTableFullName())).As(env.RelationmapTableNameColumn);
		}

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(env.ProcessnTable.GetTableFullName());
		iq.Returning(env.ProcessIdColumn);

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
