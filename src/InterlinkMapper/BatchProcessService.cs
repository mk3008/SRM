using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper;

public class BatchProcessService
{
	public BatchProcessService(IDbConnection cn, Database db, BatchTransaction trn, Datasource ds)
	{
		Connection = cn;
		DB = db;
		Transaction = trn;
		DS = ds;
	}

	private IDbConnection Connection { get; init; }

	private Database DB { get; init; }

	private string ProcessTable => DB.ProcessTableName;

	private string ProcessMapTable => DB.ProcessMapNameBuilder(DS.Destination);

	private string PlaceholderIdentifier => DB.PlaceholderIdentifier;

	private string ProcessIdColumn => DB.ProcessIdColumnName;

	private BatchTransaction Transaction { get; init; }

	private int TransactionId => Transaction.TransactionId;

	private Datasource DS { get; init; }

	private int DatasourceId => DS.DatasourceId;

	private int DestinaionId => DS.Destination.DestinationId;

	private string DestinaionIdColumn => DS.Destination.Sequence.Column;

	private BatchProcess? Process { get; set; }

	public BatchProcess Start()
	{
		if (Process != null) throw new ArgumentNullException();

		var dic = new Dictionary<string, object>();
		dic[DB.TransctionIdColumnName] = TransactionId;
		dic[DB.DestinationIdColumnName] = DestinaionId;
		dic[DB.DatasourceIdColumnName] = DatasourceId;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(ProcessTable);
		iq.Returning(ProcessIdColumn);

		var id = Connection.Execute(iq);

		Process = new BatchProcess(id, TransactionId, DS);
		return Process;
	}

	public void Mapping(SelectQuery brigeQuery)
	{
		if (Process == null) throw new ArgumentNullException();

		//with _bridge as (select ...)
		//select :process_id as process_id, b.dest_id from _bridge as b
		//where b.dest_id is not null
		var pname = PlaceholderIdentifier + ProcessIdColumn;

		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, DestinaionIdColumn);
		sq.Select(pname).As(ProcessIdColumn);
		sq.Parameters.Add(pname, Process.ProcessId);

		sq.Where(b, DestinaionIdColumn).IsNotNull();

		//insert into process map
		var iq = sq.ToInsertQuery(ProcessMapTable);

		Connection.Execute(iq);
	}

	public void DeleteDatasourceMap(SelectQuery brigeQuery)
	{
		//with _bridge as (select ...)
		//select b.destination_id from _bridge as b
		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b, DestinaionIdColumn);

		//delete from key map
		var dq = sq.ToDeleteQuery(ProcessMapTable, new[] { DestinaionIdColumn });

		Connection.Execute(dq);
	}
}
