using Carbunql;
using Carbunql.Building;
using Carbunql.Values;

namespace InterlinkMapper.TableAndMap;

public class HoldMap
{
	public List<string> DatasourceKeys { get; set; } = new();

	public string DestinationKey { get; set; } = string.Empty;

	public string HoldTableName { get; set; } = string.Empty;

	public MergeQuery GenerateMergeQuery(SelectQuery brigeQuery)
	{
		if (!DatasourceKeys.Any()) throw new Exception();
		if (string.IsNullOrEmpty(DestinationKey)) throw new ArgumentNullException(nameof(DestinationKey));
		if (string.IsNullOrEmpty(HoldTableName)) throw new ArgumentNullException(nameof(HoldTableName));

		var mq = brigeQuery.ToMergeQuery(HoldTableName, DatasourceKeys);

		// If the Sequence is numbered, remove it from the hold table
		mq.AddMatchedDelete(() => new ColumnValue(mq.DatasourceAlias, DestinationKey).IsNotNull());

		// If the Sequence is not numbered, add it to the hold table
		mq.AddNotMatchedInsert(() => new ColumnValue(mq.DatasourceAlias, DestinationKey).IsNull());

		return mq;
	}
}