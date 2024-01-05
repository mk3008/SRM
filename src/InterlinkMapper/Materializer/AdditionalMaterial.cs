using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterial : MaterializeResult
{
	public required InterlinkDatasource InterlinkDatasource { get; set; }

	public required List<string> DatasourceKeyColumns { get; set; }

	public required string KeyRelationTableFullName { get; set; }

	public required string KeyMapTableFullName { get; init; }

	public required string NumericType { get; init; }

	public required int Count { get; set; }

	internal InterlinkProcess CreateProcessAsNew()
	{
		var row = new InterlinkProcess()
		{
			InterlinkTransaction = InterlinkTransaction,
			InterlinkDatasource = InterlinkDatasource,
			ActionName = nameof(AdditionalMaterial),
			InsertCount = Count,
		};
		return row;
	}

	internal InsertQuery CreateKeyMapInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationSeqColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyMapTableFullName);
	}

	internal InsertQuery CreateKeyRelationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationSeqColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyRelationTableFullName);
	}

	public void ExecuteTransfer(IDbConnection connection)
	{
		// regist process
		var process = CreateProcessAsNew();
		connection.Save(process);

		// transfer datasource
		var cnt = connection.Execute(CreateRelationInsertQuery(process.InterlinkProcessId, KeyRelationTableFullName, DatasourceKeyColumns), commandTimeout: CommandTimeout);
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