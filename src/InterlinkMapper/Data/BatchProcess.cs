namespace InterlinkMapper.Data;

/// <summary>
/// This class represents the table that stores the relationship between the destination and the process.
/// The recommended table name is 'Destination.TableFullName + "_sync"'.
/// </summary>
public class BatchProcess
{
	public BatchProcess(int processId, int transactionId, Datasource datasource)
	{
		ProcessId = processId;
		TransactionId = transactionId;
		Datasource = datasource;
	}

	public int ProcessId { get; init; }

	public int TransactionId { get; init; }

	public int DestinationId => Datasource.Destination!.DestinationId;

	public int DatasoureId => Datasource.DatasourceId;

	public Datasource Datasource { get; init; }
}