using Carbunql.Building;
using Carbunql;
using InterlinkMapper.Data;
using System.Data;
using Carbunql.Dapper;

namespace InterlinkMapper;

public class KeyMapService
{
	public KeyMapService(IDbConnection cn, Database db, Datasource ds)
	{
		Connection = cn;
		DB = db;
		DS = ds;
	}

	private IDbConnection Connection { get; init; }

	private Database DB { get; init; }

	private string KeyMapTable => DB.KeyMapNameBuilder(DS);

	private Datasource DS { get; init; }

	private string DestinaionIdColumn => DS.Destination.Sequence.Column;

	public void Mapping(SelectQuery brigeQuery)
	{
		//WITH _bridge AS (select bridge query)
		//FROM _bridge AS b
		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");

		//SELECT b.destination_id, b.datasource_id from _bridge as b
		sq.Select(b, DestinaionIdColumn);
		DS.KeyColumns.ForEach(x => sq.Select(b, x));

		//WHERE b.destination_id is not null
		sq.Where(b, DestinaionIdColumn).IsNotNull();

		//insert into key map
		var iq = sq.ToInsertQuery(KeyMapTable);

		Connection.Execute(iq);
	}

	public void RemoveMapping(SelectQuery brigeQuery)
	{
		//WITH _bridge AS (select bridge query)
		//FROM _bridge AS b
		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("_bridge");
		var (_, b) = sq.From(bridge).As("b");

		//SELECT b.datasource_id from _bridge as b
		DS.KeyColumns.ForEach(x => sq.Select(b, x));

		//delete from key map
		var dq = sq.ToDeleteQuery(KeyMapTable, DS.KeyColumns);

		Connection.Execute(dq);
	}
}
