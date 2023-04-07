using InterlinkMapper.Data;

namespace InterlinkMapper.Tables;

public class TransactionTableMap : DbTable
{
	public string TransactionIdColumn { get; set; } = "transaction_id";

	public string DatasourceIdColumn { get; set; } = "datasource_id";

	public string DestinationIdColumn { get; set; } = "destination_id";

	public string ArgumentColumn { get; set; } = "argument";

	public override IEnumerable<string> GetColumns()
	{
		yield return TransactionIdColumn;
		yield return DatasourceIdColumn;
		yield return DestinationIdColumn;
		yield return ArgumentColumn;
	}

	public override IEnumerable<string> GetPrimaryKeyColumns()
	{
		yield return TransactionIdColumn;
	}

	public override IEnumerable<string> GetUniqueKeyColumns()
	{
		yield break;
	}

	public override string? GetSequenceColumn()
	{
		return TransactionIdColumn;
	}
}