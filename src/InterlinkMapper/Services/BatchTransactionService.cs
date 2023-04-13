using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchTransactionService
{
	public BatchTransactionService(IDbConnection connection, Database db)
	{
		Connection = connection;
		PlaceholderIdentifier = db.PlaceholderIdentifier;
		Table = db.TransctionTableName;
		IdColumn = db.TransctionIdColumnName;
		DatasourceIdColumn = db.DatasourceIdColumnName;
		DestinationIdColumn = db.DestinationIdColumnName;
		ArgumentColumn = db.ArgumentsColumnName;
	}

	public BatchTransactionService(IDbConnection connection, string placeholderIdentifier, string transactionTable, string idColumn, string datasourceIdColumnName, string destinationIdColumnName, string argumentsColumnName)
	{
		Connection = connection;
		PlaceholderIdentifier = placeholderIdentifier;
		Table = transactionTable;
		IdColumn = idColumn;
		DatasourceIdColumn = datasourceIdColumnName;
		DestinationIdColumn = destinationIdColumnName;
		ArgumentColumn = argumentsColumnName;
	}

	private IDbConnection Connection { get; init; }

	public string PlaceholderIdentifier { get; init; }

	public string Table { get; init; }

	public string IdColumn { get; init; }

	public string DatasourceIdColumn { get; init; }

	public string DestinationIdColumn { get; init; }

	public string ArgumentColumn { get; init; }

	public BatchTransaction CreateAsNew(Datasource datasource, string arguments)
	{
		var dic = new Dictionary<string, object>();
		dic[DestinationIdColumn] = datasource.Destination.DestinationId;
		dic[DatasourceIdColumn] = datasource.DatasourceId;
		dic[ArgumentColumn] = arguments;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(Table);
		iq.Returning(IdColumn);

		var id = Connection.Execute(iq);

		return new BatchTransaction(id, datasource, arguments);
	}

	//public BatchTransaction FindById(int transactinId)
	//{

	//}

	//public List<BatchTransaction> FindByDatasource(int datasourceId)
	//{

	//}

	//public List<BatchTransaction> FindByDestination(int destinationId)
	//{

	//}
}
