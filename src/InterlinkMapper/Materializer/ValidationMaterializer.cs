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

	public MaterializeResult? Create(IDbConnection connection, DbDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		if (destination.ReverseOption == null) return null;

		var requestMaterialQuery = CreateRequestMaterialTableQuery(destination);

		var requestMaterial = ExecuteMaterialQuery(connection, requestMaterialQuery);

		if (requestMaterial.Count == 0) return null;

		ExecuteDeleteOriginRequest(connection, requestMaterial, destination);
		var deleteRows = ExecuteCleanUpMaterialRequest(connection, requestMaterial, destination);

		// If all requests are deleted, there are no processing targets.
		if (requestMaterial.Count == deleteRows) return null;

		var datasourceMaterialQuery = CreateReverseDatasourceMaterialQuery(requestMaterial, destination, injector);
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

	private int ExecuteDeleteOriginRequest(IDbConnection connection, MaterializeResult result, DbDestination destination)
	{
		var query = CreateOriginDeleteQuery(result, destination);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int ExecuteCleanUpMaterialRequest(IDbConnection connection, MaterializeResult result, DbDestination destination)
	{
		var query = CleanUpMaterialRequestQuery(result, destination);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialTableQuery(DbDestination destination)
	{
		var request = Environment.GetReverseRequestTable(destination);
		var name = "__reverse_request";
		return request.ToSelectQuery().ToCreateTableQuery(name);
	}

	private DeleteQuery CreateOriginDeleteQuery(MaterializeResult result, DbDestination destination)
	{
		var request = Environment.GetReverseRequestTable(destination);
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

	private DeleteQuery CleanUpMaterialRequestQuery(MaterializeResult result, DbDestination destination)
	{
		var relation = Environment.GetRelationTable(destination);
		var relationTable = relation.Definition.TableFullName;

		var sq = new SelectQuery();
		sq.AddComment("If it does not exist in the relation table, remove it from the target");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(() =>
		{
			// not exists (select * from RELATION x where d.id = x.id)
			var q = new SelectQuery();
			var (_, x) = q.From(relationTable).As("x");
			q.Where(x, destination.Sequence.Column).Equal(r, destination.Sequence.Column);
			q.SelectAll();
			return q.ToNotExists();
		});

		sq.Select(r, destination.Sequence.Column);

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateReverseDatasourceSelectQuery(MaterializeResult request, DbDestination destination)
	{
		var reverse = Environment.GetReverseTable(destination);
		var relation = Environment.GetRelationTable(destination);
		var process = Environment.GetProcessTable();
		var op = destination.ReverseOption!;

		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(destination.ToSelectQuery()).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(x => new ColumnValue(d, destination.Sequence.Column).Equal(x.Table, destination.Sequence.Column));
		var p = f.InnerJoin(process.Definition.TableFullName).As("p").On(x => new ColumnValue(r, process.ProcessIdColumn).Equal(x.Table, process.ProcessIdColumn));

		sq.Select(d);

		//Rename the existing ID column and select it as the original ID
		var originIdSelectItem = sq.GetSelectableItems().Where(x => x.Alias.IsEqualNoCase(destination.Sequence.Column)).First();
		originIdSelectItem.SetAlias(reverse.OriginIdColumn);

		//reverse sign
		var columns = sq.GetSelectableItems();
		foreach (var column in columns)
		{
			if (op.ReverseColumns.Contains(column.Alias, StringComparer.OrdinalIgnoreCase))
			{
				var c = (ColumnValue)column.Value;
				c.AddOperatableValue("*", "-1");
			}
		};

		sq.Select(p, process.KeymapTableNameColumn);

		//exists (select * from REQUEST x where d.id = x.id)
		sq.Where(() =>
		{
			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");
			q.Where(x, destination.Sequence.Column).Equal(d, destination.Sequence.Column);
			q.SelectAll();

			return q.ToExists();
		});



		return sq;
	}

	private CreateTableQuery CreateReverseDatasourceMaterialQuery(MaterializeResult request, DbDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateReverseDatasourceSelectQuery(request, destination)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");

		sq.Select(destination.Sequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery("__reverse_datasource");
	}
}