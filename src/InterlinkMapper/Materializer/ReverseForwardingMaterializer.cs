using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseForwardingMaterializer
{
	public ReverseForwardingMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RowNumberColumnName { get; set; } = "row_num";

	public MaterializeResult? Create(IDbConnection connection, DbDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		if (!destination.AllowReverse) throw new NotSupportedException();

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
		var relation = Environment.GetRelationTable(destination);

		var sq = request.ToSelectQuery();
		var f = sq.FromClause!;
		var d = f.Root;
		var rel = f.InnerJoin(relation.Definition.TableFullName).As("rel").On(d, destination.Sequence.Column);

		sq.Select(new FunctionValue("row_number", () =>
		{
			var over = new OverClause();
			over.AddPartition(new ColumnValue(d, destination.Sequence.Column));
			over.AddOrder(new SortableItem(new ColumnValue(d, request.RequestIdColumn)));
			return over;
		})).As(RowNumberColumnName);

		var name = "__reverse_request";
		return sq.ToCreateTableQuery(name);
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
		sq.AddComment("Delete duplicate rows so that the destination ID is unique");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(r, RowNumberColumnName).NotEqual("1");

		sq.Select(r, destination.Sequence.Column);

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateReverseDatasourceSelectQuery(MaterializeResult request, DbDestination destination)
	{
		var reverse = Environment.GetReverseTable(destination);
		var relation = Environment.GetRelationTable(destination);
		var process = Environment.GetProcessTable();
		var op = destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(destination.ToSelectQuery()).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(d, destination.Sequence.Column);
		var p = f.InnerJoin(process.Definition.TableFullName).As("p").On(r, process.ProcessIdColumn);
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(d, destination.Sequence.Column);
		var rev = f.LeftJoin(reverse.Definition.TableFullName).As("rev").On(d, destination.Sequence.Column);

		sq.Select(new FunctionValue("coalesce", new ValueCollection
		{
			new ColumnValue(rev, reverse.RootIdColumn),
			new ColumnValue(d, destination.Sequence.Column)
		})).As(reverse.RootIdColumn);

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
		sq.Select(rm, reverse.RemarksColumn);

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

[GeneratePrivateProxy(typeof(ReverseForwardingMaterializer))]
public partial struct ReverseForwardingMaterializerProxy;
