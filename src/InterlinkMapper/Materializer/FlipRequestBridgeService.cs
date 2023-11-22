using Cysharp.Text;
using Dapper;
using InterlinkMapper;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;
using System.Data;
using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper.BridgeBuilder;

public class FlipRequestBridgeService : IQueryExecuteService
{
	public FlipRequestBridgeService(SystemEnvironment environment, LoggingDbConnection cn, int processId)
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

	/// <summary>
	/// Generate a bridge name.
	/// </summary>
	/// <param name="d"></param>
	/// <returns></returns>
	private string GenerateBridgeName(IDestination d)
	{
		using MD5 md5Hash = MD5.Create();

		byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(d.Table.GetTableFullName()));
		var sb = ZString.CreateStringBuilder();
		sb.Append("_flip_");
		for (int i = 0; i < 4; i++)
		{
			sb.Append(data[i].ToString("x2"));
		}
		return sb.ToString();
	}

	/// <summary>
	/// Create a new bridge table.
	/// </summary>
	/// <param name="destination"></param>
	/// <param name="injector"></param>
	/// <returns></returns>
	public (SelectQuery SelectBridgeQuery, int Rows) CreateAndSelectBridge(IDestination destination, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var bridgeName = GenerateBridgeName(destination);

		var sq = new SelectQuery();
		var (_, d) = sq.From(destination.ToSelectQueryAsFlip()).As("d");

		sq.SelectSequenceColumn(destination);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		var cnt = this.CreateTable(sq, bridgeName, isTemporary: true);

		var columns = sq.SelectClause!.Select(x => x.Alias).ToList();
		var query = bridgeName.ToSelectQuery(columns);

		return (query, cnt);
	}

	public int DeleteRequest(IDestination destination, SelectQuery bridgeQuery)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(bridgeQuery).As("d");

		sq.Select(d, destination.ReversalOption.RequestIdColumn);

		var cnt = this.Delete(sq, destination.ReversalOption.RequestTable);
		return cnt;
	}

	//public int DeleteKeymapFromBridge(IDestination destination, SelectQuery bridgeQuery, string keymapTable)
	//{
	//	var destinationIdColumn = destination.Sequence.Column;
	//	var sourceIdColumn = destination.ProcessTable.SourceIdColumnName;

	//	var sq = new SelectQuery();
	//	var (_, d) = sq.From(bridgeQuery).As("d");

	//	sq.Select(d, sourceIdColumn).As(destinationIdColumn);

	//	var cnt = this.Delete(sq, keymapTable);
	//	return cnt;
	//}


	//public List<string> GetKeymapTableNamesFromBridge(IDestination destination, SelectQuery bridgeQuery)
	//{
	//	var sq = GenerateSelectKeymapTableNameQueryFromBridge(destination, bridgeQuery);
	//	var lst = Connection.Query<string>(sq, commandTimeout: CommandTimeout).ToList();
	//	return lst;
	//}

	//private SelectQuery GenerateSelectKeymapTableNameQueryFromBridge(IDestination destinatiion, SelectQuery bridgeQuery)
	//{
	//	var keymapTableNameColumn = DbTableConfig.KeymapTableNameColumn;

	//	var (sq, processTable) = GenerateSelectQueryFromBridgeInnerJoinProcessTable(destinatiion, bridgeQuery);

	//	sq.Select(processTable, keymapTableNameColumn);
	//	sq.SelectClause!.HasDistinctKeyword = true;

	//	sq.Where(new ColumnValue(processTable, keymapTableNameColumn).IsNotNull());

	//	return sq;
	//}

	//public List<string> GetRelatinmapTableNamesFromBridge(IDestination destination, SelectQuery bridgeQuery)
	//{
	//	var sq = GenerateSelectRelationmapTableNameQueryFromBridge(destination, bridgeQuery);
	//	var lst = Connection.Query<string>(sq, commandTimeout: CommandTimeout).ToList();
	//	return lst;
	//}

	//private SelectQuery GenerateSelectRelationmapTableNameQueryFromBridge(IDestination destinatiion, SelectQuery bridgeQuery)
	//{
	//	var relationmapTableNameColumn = DbTableConfig.RelationmapTableNameColumn;

	//	var (sq, processTable) = GenerateSelectQueryFromBridgeInnerJoinProcessTable(destinatiion, bridgeQuery);

	//	sq.Select(processTable, relationmapTableNameColumn);
	//	sq.SelectClause!.HasDistinctKeyword = true;

	//	sq.Where(new ColumnValue(processTable, relationmapTableNameColumn).IsNotNull());

	//	return sq;
	//}

	//private (SelectQuery sq, SelectableTable processTable) GenerateSelectQueryFromBridgeInnerJoinProcessTable(IDestination destinatiion, SelectQuery bridgeQuery)
	//{
	//	var destinationIdColumn = destinatiion.Sequence.Column;
	//	var sourceIdColumn = destinatiion.ProcessTable.SourceIdColumnName;
	//	var processIdColumn = DbTableConfig.ProcessIdColumn;

	//	var sq = new SelectQuery();
	//	var (f, bridge) = sq.From(bridgeQuery).As("bridge");

	//	var destinationProcessTable = f.InnerJoin(destinatiion.ProcessTable.Definition.GetTableFullName()).As("dest_proc").On(x =>
	//	{
	//		return new ColumnValue(bridge, sourceIdColumn).Equal(x.Table, destinationIdColumn);
	//	});

	//	var processTable = f.InnerJoin(DbTableConfig.ProcessTable.GetTableFullName()).As("proc").On(destinationProcessTable, processIdColumn);
	//	return (sq, processTable);
	//}
}
