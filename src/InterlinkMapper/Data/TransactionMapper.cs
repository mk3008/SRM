using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using InterlinkMapper.Tables;
using System.Data;

namespace InterlinkMapper.TableAndMap;

public class TransactionMapper
{
	public TransactionMapper(Destination destination, TransactionTableMap transactionTable)
	{
		Destination = destination;
		TransactionTable = transactionTable;
	}

	public Destination Destination { get; set; }

	public TransactionTableMap TransactionTable { get; set; }

	public int Execute(IDbConnection connection, int transactionId, Datasource datasource, string argument, string placeholderIdentifier = ":")
	{
		var q = GenerateInserQuery(transactionId, datasource, argument, placeholderIdentifier);
		return connection.Execute(q);
	}

	public InsertQuery GenerateInserQuery(int transactionId, Datasource datasource, string argument, string placeholderIdentifier = ":")
	{
		// select :transaction_id as transaction_id, :destination_id as destination_id, :datasoure_id as datasource_id, :remarks as remarks
		var keyvalues = new Dictionary<string, object>
		{
			{ TransactionTable.TransactionIdColumn, transactionId },
			{ TransactionTable.DestinationIdColumn, Destination.DestinationId },
			{ TransactionTable.DatasourceIdColumn, datasource.DatasourceId },
			{ TransactionTable.ArgumentColumn, argument }
		};

		var sq = new SelectQuery();
		foreach (var item in keyvalues)
		{
			var pname = placeholderIdentifier + item.Key;
			sq.Select(pname).As(item.Key);
			sq.Parameters.Add(pname, item.Value);
		}

		return TransactionTable.ConvertToInsertQuery(sq);
	}
}