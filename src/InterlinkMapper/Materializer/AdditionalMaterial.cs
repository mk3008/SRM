using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterial : MaterializeResult
{
	public required string KeyMapTable { get; init; }

	internal InsertQuery CreateProcessInsertQuery(long transactionId)
	{
		var sq = new SelectQuery();
		sq.Select(PlaceHolderIdentifer, InterlinkTransactionIdColumn, transactionId);
		sq.Select(PlaceHolderIdentifer, InterlinkDatasourceIdColumn, InterlinkDatasourceId);
		sq.Select(PlaceHolderIdentifer, InterlinkDestinationIdColumn, InterlinkDestinationId);
		sq.Select(PlaceHolderIdentifer, KeyMapTableNameColumn, KeyMapTable);
		sq.Select(PlaceHolderIdentifer, KeyRelationTableNameColumn, KeyRelationTable);
		sq.Select(PlaceHolderIdentifer, ActionColumn, "additional");
		sq.Select(PlaceHolderIdentifer, InsertCountColumn, Count);

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(ProcessTableName);
		iq.Returning(InterlinkProcessIdColumn);

		return iq;
	}

	public required string NumericType { get; init; }

	internal InsertQuery CreateKeyMapInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyMapTable);
	}

	internal InsertQuery CreateKeyRelationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyRelationTable);
	}

	public void ExecuteTransfer(IDbConnection connection, long transactionId)
	{
		// regist process
		var processId = connection.ExecuteScalar<long>(CreateProcessInsertQuery(transactionId));

		// transfer datasource
		var cnt = connection.Execute(CreateRelationInsertQuery(processId), commandTimeout: CommandTimeout);
		if (cnt != Count) throw new InvalidOperationException();
		cnt = connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);
		if (cnt != Count) throw new InvalidOperationException();

		// create system relation mapping
		cnt = connection.Execute(CreateKeyMapInsertQuery(), commandTimeout: CommandTimeout);
		if (cnt != Count) throw new InvalidOperationException();
		cnt = connection.Execute(CreateKeyRelationInsertQuery(), commandTimeout: CommandTimeout);
		if (cnt != Count) throw new InvalidOperationException();
	}
}

[GeneratePrivateProxy(typeof(AdditionalMaterial))]
public partial struct AdditionalMaterialProxy;