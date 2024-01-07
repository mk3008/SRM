using Carbunql.Tables;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterializer : IMaterializer
{
	public AdditionalMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RequestMaterialName { get; set; } = "__additional_request";

	public string DatasourceMaterialName { get; set; } = "__additional_datasource";

	public AdditionalMaterial? Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var requestMaterialQuery = CreateRequestMaterialQuery(datasource);
		var request = this.CreateMaterial(connection, transaction, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, datasource, request);
		CleanUpRequestMaterial(connection, request, datasource);

		var query = CreateAdditionalMaterialQuery(datasource, request, injector);
		var additional = this.CreateMaterial(connection, transaction, query);

		return ToAdditionalMaterial(datasource, additional);
	}

	private void CleanUpRequestMaterial(IDbConnection connection, Material request, InterlinkDatasource datasource)
	{
		var query = CreateCleanUpRequestMaterialQuery(request, datasource);
		connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private DeleteQuery CreateCleanUpRequestMaterialQuery(Material request, InterlinkDatasource datasource)
	{
		var req = datasource.GetInsertRequestTable(Environment);

		var keys = datasource.KeyColumns.Select(x => x.ColumnName).ToList();
		var keymap = datasource.GetKeyMapTable(Environment);

		var sq = new SelectQuery();
		sq.AddComment("The data existing in KeyMap has been transformed, so delete it.");
		var (f, d) = sq.From(request.SelectQuery).As("d");
		var km = f.InnerJoin(keymap.Definition.TableFullName).As("keymap").On(d, keys);

		sq.Select(d, req.RequestIdColumn);

		return sq.ToDeleteQuery(request.MaterialName);
	}

	public AdditionalMaterial Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, Material request)
	{
		var query = CreateAdditionalMaterialQuery(datasource, request);
		var additional = this.CreateMaterial(connection, transaction, query);

		return ToAdditionalMaterial(datasource, additional);
	}

	private AdditionalMaterial ToAdditionalMaterial(InterlinkDatasource datasource, Material material)
	{
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var sourceId = source.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkDatasource.InterlinkDatasourceId)).First();

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);
		var keymap = datasource.GetKeyMapTable(Environment);
		var keyrelation = datasource.GetKeyRelationTable(Environment);

		return new AdditionalMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			DatasourceKeyColumns = datasource.KeyColumns.Select(x => x.ColumnName).ToList(),
			RootIdColumn = relation.RootIdColumn,
			OriginIdColumn = relation.OriginIdColumn,
			InterlinkRemarksColumn = relation.RemarksColumn,
			DestinationTable = datasource.Destination.DbTable.TableFullName,
			DestinationColumns = datasource.Destination.DbTable.ColumnNames,
			DestinationSeqColumn = datasource.Destination.DbSequence.ColumnName,
			KeyMapTableFullName = keymap.Definition.TableFullName,
			KeyRelationTableFullName = keyrelation.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkRelationTable = relation.Definition.TableFullName,
			NumericType = Environment.DbEnvironment.NumericTypeName,
			//ActionColumn = proc.ActionNameColumn,
			InterlinkDatasourceIdColumn = sourceId.ColumnName,
			//InsertCountColumn = proc.InsertCountColumn,
			//KeyMapTableNameColumn = proc.KeyMapTableNameColumn,
			//KeyRelationTableNameColumn = proc.KeyRelationTableNameColumn,
			//ProcessTableName = proc.Definition.TableFullName,
			InterlinkTransaction = material.InterlinkTransaction,
			//InterlinkDatasourceId = datasource.InterlinkDatasourceId,
			//InterlinkDestinationId = datasource.Destination.InterlinkDestinationId,
			InterlinkDatasource = datasource,
			Environment = Environment,
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDatasource datasource, Material result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDatasource datasource)
	{
		var request = datasource.GetInsertRequestTable(Environment);
		var relation = datasource.GetKeyRelationTable(Environment);

		var sq = new SelectQuery();
		var (f, r) = sq.From(request.Definition.TableFullName).As("r");

		sq.Select(r, request.RequestIdColumn);

		var args = new ValueCollection();
		datasource.KeyColumns.ForEach(key =>
		{
			args.Add(new ColumnValue(r, key.ColumnName));
			sq.Select(r, key.ColumnName);
		});

		return sq.ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(InterlinkDatasource datasource, Material result)
	{
		var request = datasource.GetInsertRequestTable(Environment);
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

	private SelectQuery CreateAdditionalDatasourceSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			InjectRequestMaterialFilter(cte, datasource, request);
		}

		var sq = new SelectQuery();
		var (f, d) = sq.From(ds).As("d");
		sq.Select(d);
		sq = InjectRequestMaterialFilter(sq, datasource, request);

		return sq;
	}

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, InterlinkDatasource datasource, Material request)
	{
		sq.AddComment("inject request material filter");

		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);
		var f = sq.FromClause!;
		var d = f.Root;

		var rm = f.InnerJoin(request.MaterialName).As("rm").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(d, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		return sq;
	}

	private CreateTableQuery CreateAdditionalMaterialQuery(InterlinkDatasource datasource, Material request)
	{
		return CreateAdditionalMaterialQuery(datasource, request, null);
	}

	private CreateTableQuery CreateAdditionalMaterialQuery(InterlinkDatasource datasource, Material request, Func<SelectQuery, SelectQuery>? injector = null)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateAdditionalDatasourceSelectQuery(datasource, request)).As("_target_datasource");

		var (_, d) = sq.From(_datasource).As("d");
		sq.Select(datasource.Destination.DbSequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}
}

[GeneratePrivateProxy(typeof(AdditionalMaterializer))]
public partial struct AdditionaMaterializerProxy;