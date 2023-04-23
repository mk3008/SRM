namespace InterlinkMapper.Data;

public class ProcessTable : DbTable
{
	public string ProcessIdColumnName { get; set; } = string.Empty;

	public string TransactionIdColumnName { get; set; } = string.Empty;
}
