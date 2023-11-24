using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using RedOrb;

namespace InterlinkMapper.Services;

public class AdditionalForwardingService
{
	public AdditionalForwardingService(SystemEnvironment environment) //, LoggingDbConnection cn)
	{
		Environment = environment;
		//Connection = cn;
	}

	//private LoggingDbConnection Connection { get; init; }

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(LoggingDbConnection cn, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var transaction = GetTransaction(cn, datasource);

		var bridge = CreateBridge(cn, datasource, injector);
		if (bridge == null || bridge.Count == 0) return;

		var process = GetProcess(cn, transaction, datasource, bridge.Count);

		InsertToDestination(cn, datasource, bridge);
		InsertToKeymap(cn, datasource, bridge);
		InsertToRelation(cn, datasource, process, bridge);
	}

	private MaterializeResult? CreateBridge(LoggingDbConnection cn, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var service = new AdditionalForwardingMaterializer(Environment);
		return service.Create(cn, datasource, injector);
	}

	private TransactionRow GetTransaction(LoggingDbConnection cn, DbDatasource datasource)
	{
		var service = new BatchTransactionService(Environment, cn);
		return service.Regist(datasource);
	}

	private ProcessRow GetProcess(LoggingDbConnection cn, TransactionRow transaction, DbDatasource datasource, int rows)
	{
		var service = new BatchProcessService(Environment, cn);
		return service.Regist(transaction, datasource, nameof(AdditionalForwardingService), rows);
	}

	private int InsertToDestination(LoggingDbConnection cn, DbDatasource datasource, MaterializeResult bridge)
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

		return cn.Execute(sq.ToInsertQuery(datasource.Destination.Table.GetTableFullName()));
	}

	private int InsertToRelation(LoggingDbConnection cn, DbDatasource datasource, ProcessRow process, MaterializeResult bridge)
	{
		var relation = Environment.GetRelationTable(datasource.Destination);

		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge.SelectQuery).As("d");

		sq.Select(relation.DestinationSequenceColumn);
		sq.Select(Environment.DbEnvironment, relation.ProcessIdColumn, process.ProcessId);

		return cn.Execute(sq.ToInsertQuery(relation.Definition.TableFullName));
	}

	private int InsertToKeymap(LoggingDbConnection cn, DbDatasource datasource, MaterializeResult bridge)
	{
		var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		var (_, d) = sq.From(bridge.SelectQuery).As("d");

		sq.Select(keymap.DestinationSequenceColumn);
		keymap.DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return cn.Execute(sq.ToInsertQuery(datasource.Destination.Table.GetTableFullName()));
	}
}
