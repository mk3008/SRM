using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseMaterial : DatasourceMaterial
{
	public required int Count { get; set; }

	internal void ExecuteTransfer(IDbConnection connection)
	{
		var datasources = SelectDatasources(connection);

		foreach (var datasource in datasources)
		{
			var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();

			var sq = new SelectQuery();
			var (f, d) = sq.From(SelectQuery).As("d");
			var keymap = f.InnerJoin(datasource.GetKeyMapTable(Environment).Definition.GetTableFullName()).As("keymap").On(x =>
			{
				x.Condition(d, OriginIdColumn).Equal(x.Table, datasource.Destination.DbSequence.ColumnName);
			});
			sq.Where(d, source.GetSequence().ColumnName).Equal(datasource.InterlinkDatasourceId.ToString());
			sq.Select(d);
			datasource.KeyColumns.ForEach(x => sq.Select(keymap, x.ColumnName));

			var m = new DatasourceReverseMaterial
			{
				CommandTimeout = CommandTimeout,
				DestinationColumns = DestinationColumns,
				DestinationIdColumn = DestinationIdColumn,
				DestinationTable = DestinationTable,
				Environment = Environment,
				InterlinkDatasource = datasource,
				InterlinkDatasourceIdColumn = InterlinkDatasourceIdColumn,
				InterlinkProcessIdColumn = InterlinkProcessIdColumn,
				InterlinkRelationTable = InterlinkRelationTable,
				InterlinkRemarksColumn = InterlinkRemarksColumn,
				InterlinkTransaction = InterlinkTransaction,
				MaterialName = MaterialName,
				OriginIdColumn = OriginIdColumn,
				PlaceHolderIdentifer = PlaceHolderIdentifer,
				RootIdColumn = RootIdColumn,
				SelectQuery = sq,
			};

			m.ExecuteTransfer(connection);
		}
	}

	private List<InterlinkDatasource> SelectDatasources(IDbConnection connection)
	{
		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();

		return connection.Load<InterlinkDatasource>(query =>
		{
			var seqColumn = source.GetSequence().ColumnName;

			var sq = new SelectQuery();
			sq.AddComment("filterd by request");
			var (_, x) = sq.From(SelectQuery).As("x");
			sq.SelectAll();
			sq.Where(query.FromClause!, seqColumn).Equal(x, seqColumn);

			query.Where(sq.ToExists());
		});
	}
}

[GeneratePrivateProxy(typeof(ReverseMaterial))]
public partial struct ReverseMaterialProxy;
