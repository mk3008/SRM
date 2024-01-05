using InterlinkMapper.Models;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

internal class DatasourceReverseMaterial : MaterializeResult
{
	public required InterlinkDatasource InterlinkDatasource { get; set; }

	internal void ExecuteTransfer(IDbConnection connection)
	{
		var count = SelectCount(connection);
		if (count == 0) return;

		var process = CreateProcessAsNew(count);
		connection.Save(process);

		var keyrelation = InterlinkDatasource.GetKeyRelationTable(Environment).Definition.GetTableFullName();
		var keycolumns = InterlinkDatasource.KeyColumns.Select(x => x.ColumnName).ToList();

		// transfer datasource
		var cnt = connection.Execute(CreateRelationInsertQuery(process.InterlinkProcessId, keyrelation, keycolumns), commandTimeout: CommandTimeout);
		if (cnt != count) throw new InvalidOperationException();
		cnt = connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);
		if (cnt != count) throw new InvalidOperationException();

		// create system relation mapping
		cnt = connection.Execute(CreateKeyRelationInsertQuery(process), commandTimeout: CommandTimeout);
		if (cnt != count) throw new InvalidOperationException();
		cnt = connection.Execute(CreateKeyMapDeleteQuery(process), commandTimeout: CommandTimeout);
		if (cnt != count) throw new InvalidOperationException();
	}

	private InterlinkProcess CreateProcessAsNew(long count)
	{
		var row = new InterlinkProcess()
		{
			InterlinkTransaction = InterlinkTransaction,
			InterlinkDatasource = InterlinkDatasource,
			ActionName = nameof(ReverseMaterial),
			InsertCount = count,
		};
		return row;
	}

	internal int SelectCount(IDbConnection connection)
	{
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();

		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		//sq.Where(d, source.GetSequence().ColumnName).Equal(InterlinkDatasource.InterlinkDatasourceId.ToString());
		sq.Select("count(*)");

		return connection.ExecuteScalar<int>(sq);
	}

	private InsertQuery CreateKeyRelationInsertQuery(InterlinkProcess proc)
	{
		var keycolumns = InterlinkDatasource.KeyColumns.Select(x => x.ColumnName).ToList();

		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(proc)).As("d");

		sq.Select(d, DestinationSeqColumn);
		keycolumns.ForEach(x => sq.Select(d, x));

		return sq.ToInsertQuery(proc.InterlinkDatasource.GetKeyRelationTable(Environment).Definition.TableFullName);
	}

	private DeleteQuery CreateKeyMapDeleteQuery(InterlinkProcess proc)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateMaterialSelectQuery(proc)).As("d");

		sq.Select(d, OriginIdColumn).As(DestinationSeqColumn);

		return sq.ToDeleteQuery(proc.InterlinkDatasource.GetKeyMapTable(Environment).Definition.TableFullName);
	}

	private SelectQuery CreateMaterialSelectQuery(InterlinkProcess proc)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");
		//sq.Where(d, InterlinkDatasourceIdColumn).Equal(proc.InterlinkDatasource.InterlinkDatasourceId.ToString());

		sq.Select(d);

		return sq;
	}
}