using InterlinkMapper.Data;

namespace InterlinkMapper.Tables;

public class ProcessTableMap : DbTable
{
	public ProcessTableMap(TransactionTableMap transactionTable)
	{
		TransactionTable = transactionTable;
	}

	public string ProcessIdColumn { get; set; } = "process_id";

	public TransactionTableMap TransactionTable { get; init; }

	public override IEnumerable<string> GetColumns()
	{
		yield return ProcessIdColumn;
		yield return TransactionTable.TransactionIdColumn;
		yield return TransactionTable.DatasourceIdColumn;
		yield return TransactionTable.DestinationIdColumn;
	}

	public override IEnumerable<string> GetPrimaryKeyColumns()
	{
		yield return ProcessIdColumn;
	}

	public override IEnumerable<string> GetUniqueKeyColumns()
	{
		yield break;
	}

	public override string? GetSequenceColumn()
	{
		return ProcessIdColumn;
	}
}