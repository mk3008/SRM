using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper.Services;

public class BridgeService
{

	public BridgeService(IDbConnection cn)
	{
		Connection = cn;
	}

	private IDbConnection Connection { get; init; }

	public SelectQuery CreateAsNew(Datasource datasource, string bridgeName, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var q = GetDatasourceQuery(datasource);

		var sq = new SelectQuery();
		var ds = sq.With(q).As("_datasource");
		var (_, d) = sq.From(ds).As("d");

		var seq = datasource.Destination.Sequence;
		sq.Select(seq.Command).As(seq.Column);
		sq.Select(d);

		if (injector != null) sq = injector(sq);

		var columns = new List<string>();
		sq.SelectClause!.ToList().ForEach(x => columns.Add(x.Alias));

		var cq = sq.ToCreateTableQuery(bridgeName, isTemporary: true);

		Connection.Execute(cq);

		return GetSelectQuery(bridgeName, columns);
	}

	private SelectQuery GetDatasourceQuery(Datasource ds)
	{
		//WITH _full_datasource as (SELECT v1, v2, ...)
		//SELECT d.v1, d.v2, ... FROM _datasource AS d
		var (sq, cteDatasource) = new SelectQuery(ds.Query).ToCTE("_full_datasource");
		var (f, d) = sq.From(cteDatasource).As("d");
		sq.Select(d);

		return sq;
	}

	private SelectQuery GetSelectQuery(string bridgeName, List<string> columns)
	{
		var sq = new SelectQuery();
		var (_, b) = sq.From(bridgeName).As("b");

		columns.ForEach(x => sq.Select(b, x));

		return sq;
	}
}
