namespace InterlinkMapper.Data;

public class BatchTransaction
{
	public BatchTransaction(int transactionId, Datasource datasource, string arguments)
	{
		TransactionId = transactionId;
		Datasource = datasource;
		Arguments = arguments;
	}

	public int TransactionId { get; init; }

	public int DestinationId => Datasource.Destination!.DestinationId;

	public int DatasoureId => Datasource.DatasourceId;

	public Datasource Datasource { get; init; }

	public string Arguments { get; init; } = string.Empty;
}
