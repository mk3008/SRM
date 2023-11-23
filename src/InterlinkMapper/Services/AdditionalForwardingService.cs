using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using RedOrb;

namespace InterlinkMapper.Services;

public class AdditionalForwardingService
{
	public AdditionalForwardingService(SystemEnvironment environment, LoggingDbConnection cn)
	{
		Environment = environment;
		Connection = cn;
	}

	private LoggingDbConnection Connection { get; init; }

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var bridge = GetBridge(datasource, injector);

		var transaction = GetTransaction(datasource);
		var process = GetProcess(transaction, datasource, bridge.Count);

		if (bridge.Count == 0) return;

		InsertToDestination(datasource, bridge);
		InsertToKeymap(datasource, bridge);
		InsertToRelation(datasource, process, bridge);
	}

	private MaterializeResult GetBridge(DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var service = new AdditionalForwardingBridgeService(Environment);
		return service.Create(Connection, datasource, injector);
	}

	private TransactionRow GetTransaction(DbDatasource datasource)
	{
		var service = new BatchTransactionService(Environment, Connection);
		return service.Regist(datasource);
	}

	private ProcessRow GetProcess(TransactionRow transaction, DbDatasource datasource, int rows)
	{
		var service = new BatchProcessService(Environment, Connection);
		return service.Regist(transaction, datasource, nameof(AdditionalForwardingService), rows);
	}

	private int InsertToDestination(DbDatasource datasource, MaterializeResult bridge)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge.SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		var columns = sq.GetSelectableItems().ToList();
		foreach (var item in columns.Where(x => !datasource.Destination.Table.Columns.Contains(x.Alias, StringComparer.OrdinalIgnoreCase)))
		{
			sq.SelectClause!.Remove(item);
		}

		return Connection.Execute(sq.ToInsertQuery(datasource.Destination.Table.GetTableFullName()));
	}

	private int InsertToRelation(DbDatasource datasource, ProcessRow process, MaterializeResult bridge)
	{
		var relation = Environment.GetRelationTable(datasource.Destination);

		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge.SelectQuery).As("d");

		sq.Select(relation.DestinationSequenceColumn);
		sq.Select(Environment.DbEnvironment, relation.ProcessIdColumn, process.ProcessId);

		return Connection.Execute(sq.ToInsertQuery(relation.Definition.TableFullName));
	}

	private int InsertToKeymap(DbDatasource datasource, MaterializeResult bridge)
	{
		var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge.SelectQuery).As("d");

		sq.Select(keymap.DestinationSequenceColumn);
		keymap.DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return Connection.Execute(sq.ToInsertQuery(datasource.Destination.Table.GetTableFullName()));
	}
}
