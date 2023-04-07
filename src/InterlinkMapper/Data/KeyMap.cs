using Carbunql;
using Carbunql.Building;
using InterlinkMapper.Tables;

namespace InterlinkMapper.TableAndMap;

public class KeyMap : KeyTable
{
	public KeyTable? KeyTable { get; set; }

	public string DestinationKey { get; set; } = string.Empty;

	public InsertQuery GenerateInserQuery(SelectQuery brigeQuery)
	{
		if (KeyTable == null) throw new Exception();

		// with bridge as (...) 
		// select b.seq, b.key1, ..., b.keyN from bridge as b
		// where b.seq is not null
		var sq = new SelectQuery();
		var bridge = sq.With(brigeQuery).As("bridge");
		var (_, b) = sq.From(bridge).As("b");
		sq.Select(b);
		sq.SelectClause!.FilterInColumns(KeyTable.GetColumns());
		sq.Where(b, DestinationKey).IsNotNull();

		return KeyTable.ConvertToInsertQuery(sq);
	}
}