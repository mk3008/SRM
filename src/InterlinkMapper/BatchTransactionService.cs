using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper;

public class BatchTransactionService
{
	public BatchTransactionService(IDbConnection connection, Database database, Datasource ds, string arguments)
	{
		Connection = connection;
		DB = database;
		DS = ds;
		Arguments = arguments;
	}

	private IDbConnection Connection { get; init; }

	private Database DB { get; init; }

	private string PlaceholderIdentifier => DB.PlaceholderIdentifier;

	private string TransctionIdColumn => DB.TransctionIdColumnName;

	private string TransctionTable => DB.TransctionTableName;

	private Datasource DS { get; init; }

	private string Arguments { get; init; }

	public BatchTransaction Start()
	{
		var dic = new Dictionary<string, object>();
		dic[DB.DestinationIdColumnName] = DS.Destination.DestinationId;
		dic[DB.DatasourceIdColumnName] = DS.DatasourceId;
		dic[DB.ArgumentsColumnName] = Arguments;

		var iq = dic.ToSelectQuery(PlaceholderIdentifier).ToInsertQuery(TransctionTable);
		iq.Returning(TransctionIdColumn);

		var id = Connection.Execute(iq);

		return new BatchTransaction(id, DS, Arguments);
	}

}
