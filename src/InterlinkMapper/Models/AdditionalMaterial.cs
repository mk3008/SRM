using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterial : DatasourceMaterial
{
	public required InterlinkDatasource InterlinkDatasource { get; init; }

	public required List<string> DatasourceKeyColumns { get; init; }

	public required string KeyRelationTableFullName { get; init; }

	public required string KeyMapTableFullName { get; init; }

	public required string NumericType { get; init; }

	public required int Count { get; init; }

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

	private InterlinkProcess CreateProcessAsNew()
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

	private InsertQuery CreateKeyMapInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyMapTableFullName);
	}

	private InsertQuery CreateKeyRelationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyRelationTableFullName);
	}
}

[GeneratePrivateProxy(typeof(AdditionalMaterial))]
public partial struct AdditionalMaterialProxy;