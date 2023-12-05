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

	public AdditionalMaterial? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var requestMaterialQuery = CreateRequestMaterialTableQuery(datasource);

		var requestMaterial = ExecuteMaterialQuery(connection, datasource, requestMaterialQuery);

		if (requestMaterial.Count == 0) return null;

		ExecuteDeleteOriginRequest(connection, datasource, requestMaterial);
		var deleteRows = ExecuteCleanUpMaterialRequest(connection, datasource, requestMaterial);

		// If all requests are deleted, there are no processing targets.
		if (requestMaterial.Count == deleteRows) return null;

		var datasourceMaterialQuery = CreateAdditionalDatasourceMaterialQuery(datasource, requestMaterial, injector);
		return ExecuteMaterialQuery(connection, datasource, datasourceMaterialQuery);
	}

	private AdditionalMaterial ExecuteMaterialQuery(IDbConnection connection, DbDatasource datasource, CreateTableQuery createTableQuery)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(datasource.Destination);
		var keymap = Environment.GetKeymapTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		var tableName = createTableQuery.TableFullName;

		connection.Execute(createTableQuery, commandTimeout: CommandTimeout);

		var rows = connection.ExecuteScalar<int>(createTableQuery.ToCountQuery());

		var sq = createTableQuery.ToSelectQuery();
		if (!sq.SelectClause!.Where(x => x.Alias == reverse.RootIdColumn).Any())
		{
			sq.Select("null::int8").As(reverse.RootIdColumn);
		};
		if (!sq.SelectClause!.Where(x => x.Alias == reverse.OriginIdColumn).Any())
		{
			sq.Select("null::int8").As(reverse.RootIdColumn);
		};
		if (!sq.SelectClause!.Where(x => x.Alias == reverse.RemarksColumn).Any())
		{
			sq.Select("null::text").As(reverse.RemarksColumn);
		};

		return new AdditionalMaterial
		{
			Count = rows,
			MaterialName = tableName,
			SelectQuery = sq,
			DatasourceKeyColumns = datasource.KeyColumns.Select(x => x.ColumnName).ToList(),
			RootIdColumn = reverse.RootIdColumn,
			OriginIdColumn = reverse.OriginIdColumn,
			RemarksColumn = reverse.RemarksColumn,
			DestinationTable = datasource.Destination.Table.GetTableFullName(),
			DestinationColumns = datasource.Destination.Table.Columns,
			DestinationIdColumn = datasource.Destination.Sequence.Column,
			KeymapTable = keymap.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			ProcessIdColumn = process.ProcessIdColumn,
			RelationTable = relation.Definition.TableFullName,
			ReverseTable = reverse.Definition.TableFullName,
		};
	}

	private int ExecuteDeleteOriginRequest(IDbConnection connection, DbDatasource datasource, MaterializeResult result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int ExecuteCleanUpMaterialRequest(IDbConnection connection, DbDatasource datasource, MaterializeResult result)
	{
		var query = CleanUpMaterialRequestQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialTableQuery(DbDatasource datasource)
	{
		var request = Environment.GetInsertRequestTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		var sq = request.ToSelectQuery();
		var f = sq.FromClause!;
		var d = f.Root;
		//var rev = f.LeftJoin(reverse.Definition.TableFullName).As("rev").On(x =>
		//{
		//	return new ColumnValue(d, request.OriginIdColumn).Equal(x.Table, reverse.ReverseIdColumn);
		//});
		var args = new ValueCollection();
		datasource.KeyColumns.ForEach(key => args.Add(new ColumnValue(d, key.ColumnName)));

		//sq.Select(rev, reverse.RootIdColumn);
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

	private DeleteQuery CreateOriginDeleteQuery(DbDatasource datasource, MaterializeResult result)
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

	private DeleteQuery CleanUpMaterialRequestQuery(DbDatasource datasource, MaterializeResult result)
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

	private SelectQuery CreateAdditionalDatasourceSelectQuery(DbDatasource datasource, MaterializeResult request)
	{
		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			InjectRequestMaterialFilter(cte, datasource, request);
			//return ds;
		}

		var sq = new SelectQuery();
		var (f, d) = sq.From(ds).As("d");
		sq.Select(d);
		sq = InjectRequestMaterialFilter(sq, datasource, request);

		return sq;
	}

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, DbDatasource datasource, MaterializeResult request)
	{
		sq.AddComment("inject request material filter");

		var reverse = Environment.GetReverseTable(datasource.Destination);
		var f = sq.FromClause!;
		var d = f.Root;

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

	private CreateTableQuery CreateAdditionalDatasourceMaterialQuery(DbDatasource datasource, MaterializeResult request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateAdditionalDatasourceSelectQuery(datasource, request)).As("_target_datasource");

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