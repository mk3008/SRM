using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class BatchProcessService
{
	public BatchProcessService(IDbConnection cn, Database db)
	{
		Connection = cn;

		PlaceholderIdentifier = db.PlaceholderIdentifier;
		Table = db.ProcessTableName;
		IdColumn = db.ProcessIdColumnName;
		TransactionIdColumn = db.TransctionIdColumnName;
		DatasourceIdColumn = db.DatasourceIdColumnName;
		DestinationIdColumn = db.DestinationIdColumnName;
	}

	private IDbConnection Connection { get; init; }

	public string PlaceholderIdentifier { get; init; }

	public string Table { get; init; }

	public string IdColumn { get; init; }

	public string TransactionIdColumn { get; init; }

	public string DatasourceIdColumn { get; init; }

	public string DestinationIdColumn { get; init; }

	public BatchProcess CreateAsNew(int transactionId, Datasource datasource)
	{
		var dic = new Dictionary<string, object>();
		dic[TransactionIdColumn] = transactionId;
		dic[DestinationIdColumn] = datasource.Destination.DestinationId;
		dic[DatasourceIdColumn] = datasource.DatasourceId;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(Table);
		iq.Returning(IdColumn);

		var id = Connection.Execute(iq);

		return new BatchProcess(id, transactionId, datasource);
	}
}
