using InterlinkMapper.Models;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Services;

/// <summary>
/// Compare the transferred data source with the current data source 
/// and add reversed records if there are differences
/// </summary>
public class FlipTransferService : IQueryExecuteService
{
	public FlipTransferService(SystemEnvironment environment, LoggingDbConnection cn, int processId)
	{
		Environment = environment;
		Connection = cn;
		ProcessId = processId;
	}

	public LoggingDbConnection Connection { get; init; }

	public SystemEnvironment Environment { get; init; }

	private DbEnvironment DbQueryConfig => Environment.DbEnvironment;

	private DbTableConfig DbTableConfig => Environment.DbTableConfig;

	public int ProcessId { get; init; }

	public int CommandTimeout { get; set; } = 60 * 5;

	public int TransferToDestinationFromBridge(IDestination destination, SelectQuery bridgeQuery, string datasourceKey)
	{
		TransferToProcessMap(destination, bridgeQuery, datasourceKey);

		var sq = GenerateSelectQuery(destination, bridgeQuery, datasourceKey);
		var cnt = this.Insert(sq, destination.Table);
		return cnt;
	}

	private int TransferToProcessMap(IDestination destination, SelectQuery bridgeQuery, string datasourceKey)
	{
		var sq = GenerateSelectQuery(destination, bridgeQuery, datasourceKey);



		var cnt = this.Insert(sq, destination.ProcessTable.Definition);
		return cnt;
	}

	private SelectQuery GenerateSelectQuery(IDestination destination, SelectQuery bridgeQuery, string datasourceKey)
	{
		var sq = new SelectQuery();
		var (_, bridge) = sq.From(bridgeQuery).As("bridge");
		sq.Select(bridge);
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, "datasource_key", datasourceKey);
		sq.Select(DbQueryConfig.PlaceHolderIdentifer, "process_id", ProcessId);
		sq.Select("true").As(destination.ProcessTable.FlipFlagColumnName);

		return sq;
	}

	public int DeleteKeymapAsSuccess(IDestination destination, SelectQuery bridgeQuery, string keymapTable)
	{
		var destinationIdColumn = destination.Sequence.Column;
		var sourceIdColumn = destination.ProcessTable.DatasourceIdColumn;

		var sq = new SelectQuery();
		var (_, bridge) = sq.From(bridgeQuery).As("bridge");

		sq.Select(bridge, sourceIdColumn).As(destinationIdColumn);

		var cnt = this.Delete(sq, keymapTable);
		return cnt;
	}

	public int TransferToRelationmapFromBridge(IDestination destination, SelectQuery bridgeQuery, string relationmapTable)
	{
		var destinationIdColumn = destination.Sequence.Column;
		var sourceIdColumn = destination.ProcessTable.DatasourceIdColumn;

		var sq = new SelectQuery();
		var (f, bridge) = sq.From(bridgeQuery).As("bridge");
		var sourceDestRelation = f.InnerJoin(relationmapTable).As("source_dest_relation").On(x =>
		{
			return new ColumnValue(bridge, sourceIdColumn).Equal(x.Table.Alias, destinationIdColumn);
		});

		sq.Select(bridge, destinationIdColumn);
		sq.Select(sourceDestRelation, destinationIdColumn);


		var cnt = this.Insert(sq, relationmapTable);
		return cnt;
	}
}
