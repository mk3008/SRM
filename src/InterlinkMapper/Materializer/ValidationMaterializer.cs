using InterlinkMapper.Models;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ValidationMaterializer
{
	public ValidationMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RequestMaterialName { get; set; } = "__validation_request";

	public string DatasourceMaterialName { get; set; } = "__validation_datasource";

	public MaterializeResult? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		if (datasource.Destination.ReverseOption == null) throw new NotSupportedException();

		var requestMaterialQuery = CreateRequestMaterialTableQuery(datasource);

		var requestMaterial = ExecuteMaterialQuery(connection, requestMaterialQuery);

		if (requestMaterial.Count == 0) return null;

		ExecuteDeleteOriginRequest(connection, requestMaterial, datasource);
		var deleteRows = ExecuteCleanUpMaterialRequest(connection, requestMaterial, datasource);

		// If all requests are deleted, there are no processing targets.
		if (requestMaterial.Count == deleteRows) return null;

		var datasourceMaterialQuery = CreateValidationDatasourceMaterialQuery(requestMaterial, datasource, injector);
		return ExecuteMaterialQuery(connection, datasourceMaterialQuery);
	}

	private MaterializeResult ExecuteMaterialQuery(IDbConnection connection, CreateTableQuery createTableQuery)
	{
		var tableName = createTableQuery.TableFullName;

		connection.Execute(createTableQuery, commandTimeout: CommandTimeout);

		var rows = connection.ExecuteScalar<int>(createTableQuery.ToCountQuery());

		return new MaterializeResult
		{
			Count = rows,
			MaterialName = tableName,
			SelectQuery = createTableQuery.ToSelectQuery(),
		};
	}

	private int ExecuteDeleteOriginRequest(IDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CreateOriginDeleteQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int ExecuteCleanUpMaterialRequest(IDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CleanUpMaterialRequestQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialTableQuery(DbDatasource datasource)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		return request.ToSelectQuery().ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(MaterializeResult result, DbDatasource datasource)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		var requestTable = request.Definition.TableFullName;
		var requestId = request.RequestIdColumn;

		var sq = new SelectQuery();
		sq.AddComment("data that has been materialized will be deleted from the original.");

		var (f, r) = sq.From(requestTable).As("r");

		sq.Where(() =>
		{
			// exists (select * from REQUEST x where d.key = x.key)
			var q = new SelectQuery();
			var (_, x) = q.From(result.MaterialName).As("x");
			q.Where(x, requestId).Equal(r, requestId);
			q.SelectAll();
			return q.ToExists();
		});

		sq.Select(r, requestId);

		return sq.ToDeleteQuery(requestTable);
	}

	private DeleteQuery CleanUpMaterialRequestQuery(MaterializeResult result, DbDatasource datasource)
	{
		var relation = Environment.GetRelationTable(datasource.Destination);
		var relationTable = relation.Definition.TableFullName;

		var sq = new SelectQuery();
		sq.AddComment("If it does not exist in the relation table, remove it from the target");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(() =>
		{
			// not exists (select * from RELATION x where d.id = x.id)
			var q = new SelectQuery();
			var (_, x) = q.From(relationTable).As("x");
			q.Where(x, datasource.Destination.Sequence.Column).Equal(r, datasource.Destination.Sequence.Column);
			q.SelectAll();
			return q.ToNotExists();
		});

		sq.Select(r, datasource.Destination.Sequence.Column);

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateValidationDatasourceSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var validation = Environment.GetValidationRequestTable(datasource);
		var relation = Environment.GetRelationTable(datasource.Destination);
		var process = Environment.GetProcessTable();
		var op = datasource.Destination.ReverseOption!;

		var sq = new SelectQuery();
		sq.AddComment("data source to be reverse");
		var (f, d) = sq.From(datasource.Destination.ToSelectQuery()).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(x => new ColumnValue(d, datasource.Destination.Sequence.Column).Equal(x.Table, datasource.Destination.Sequence.Column));
		var p = f.InnerJoin(process.Definition.TableFullName).As("p").On(x => new ColumnValue(r, process.ProcessIdColumn).Equal(x.Table, process.ProcessIdColumn));

		sq.Select(d);

		sq.Select(p, process.KeymapTableNameColumn);

		//exists (select * from REQUEST x where d.id = x.id)
		sq.Where(() =>
		{
			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");
			q.Where(x, datasource.Destination.Sequence.Column).Equal(d, datasource.Destination.Sequence.Column);
			q.SelectAll();

			return q.ToExists();
		});

		return sq;
	}

	private CreateTableQuery CreateValidationDatasourceMaterialQuery(MaterializeResult request, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateValidationDatasourceSelectQuery(request, datasource)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");

		sq.Select(datasource.Destination.Sequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}
}