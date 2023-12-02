using Carbunql.Tables;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalForwardingMaterializer
{
	public AdditionalForwardingMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	public string RowNumberColumnName { get; set; } = "row_num";

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public MaterializeResult? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var requestMaterialQuery = CreateRequestMaterialTableQuery(datasource);

		var requestMaterial = ExecuteMaterialQuery(connection, requestMaterialQuery);

		if (requestMaterial.Count == 0) return null;

		ExecuteDeleteOriginRequest(connection, requestMaterial, datasource);
		var deleteRows = ExecuteCleanUpMaterialRequest(connection, requestMaterial, datasource);

		// If all requests are deleted, there are no processing targets.
		if (requestMaterial.Count == deleteRows) return null;

		var datasourceMaterialQuery = CreateAdditionalDatasourceMaterialQuery(requestMaterial, datasource, injector);
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
		var request = Environment.GetInsertRequestTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		var sq = request.ToSelectQuery();
		var f = sq.FromClause!;
		var d = f.Root;
		var rev = f.LeftJoin(reverse.Definition.TableFullName).As("rev").On(x =>
		{
			return new ColumnValue(d, request.OriginIdColumn).Equal(x.Table, reverse.ReverseIdColumn);
		});
		var args = new ValueCollection();
		datasource.KeyColumns.ForEach(key => args.Add(new ColumnValue(d, key.ColumnName)));

		sq.Select(rev, reverse.RootIdColumn);
		sq.Select(new FunctionValue("row_number", () =>
		{
			var over = new OverClause();
			over.AddPartition(args);
			over.AddOrder(new SortableItem(new ColumnValue(d, request.RequestIdColumn)));
			return over;
		})).As(RowNumberColumnName);

		var name = "__additional_request";
		return sq.ToCreateTableQuery(name);
	}

	private DeleteQuery CreateOriginDeleteQuery(MaterializeResult result, DbDatasource datasource)
	{
		var request = Environment.GetInsertRequestTable(datasource);
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
		var keyamp = Environment.GetKeymapTable(datasource);
		var keymapTable = keyamp.Definition.TableFullName;
		var datasourceKeys = keyamp.DatasourceKeyColumns;

		var sq = new SelectQuery();
		sq.AddComment("exclude requests that exist in the keymap from forwarding");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(() =>
		{
			// exists (select * from KEYMAP x where d.key = x.key)
			var q = new SelectQuery();
			var (_, x) = q.From(keymapTable).As("x");
			datasourceKeys.ForEach(key =>
			{
				q.Where(x, key).Equal(r, key);
			});
			q.SelectAll();
			return q.ToExists().Or(r, RowNumberColumnName).NotEqual("1");
		});

		datasourceKeys.ForEach(key => sq.Select(r, key));

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateAdditionalDatasourceSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			InjectRequestMaterialFilter(cte, request, datasource);
			//return ds;
		}

		var sq = new SelectQuery();
		var (f, d) = sq.From(ds).As("d");
		sq.Select(d);
		sq = InjectRequestMaterialFilter(sq, request, datasource);

		return sq;
	}

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, MaterializeResult request, DbDatasource datasource)
	{
		var reverse = Environment.GetReverseTable(datasource.Destination);
		var f = sq.FromClause!;
		var d = f.Root;

		sq.AddComment("inject request material filter");
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(d, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		sq.Select(rm, reverse.RootIdColumn);
		sq.Select(rm, reverse.OriginIdColumn);

		return sq;
	}

	private CreateTableQuery CreateAdditionalDatasourceMaterialQuery(MaterializeResult request, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateAdditionalDatasourceSelectQuery(request, datasource)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");
		sq.Select(datasource.Destination.Sequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery("__datasource");
	}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingMaterializer))]
public partial struct MaterializeServiceProxy;