using Carbunql.Tables;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
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

	public AdditionalMaterial? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var requestMaterialQuery = CreateRequestMaterialQuery(datasource);
		var request = this.CreateMaterial(connection, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, datasource, request);

		var query = CreateAdditionalMaterialQuery(datasource, request, injector);
		var additional = this.CreateMaterial(connection, query);

		return ToAdditionalMaterial(datasource, additional);
	}

	public AdditionalMaterial Create(IDbConnection connection, DbDatasource datasource, Material request)
	{
		var query = CreateAdditionalMaterialQuery(datasource, request);
		var additional = this.CreateMaterial(connection, query);

		return ToAdditionalMaterial(datasource, additional);
	}

	private AdditionalMaterial ToAdditionalMaterial(DbDatasource datasource, Material material)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(datasource.Destination);
		var keymap = Environment.GetKeyMapTable(datasource);
		var history = Environment.GetKeyRelationTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		return new AdditionalMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			DatasourceKeyColumns = datasource.KeyColumns.Select(x => x.ColumnName).ToList(),
			RootIdColumn = reverse.RootIdColumn,
			OriginIdColumn = reverse.OriginIdColumn,
			RemarksColumn = reverse.RemarksColumn,
			DestinationTable = datasource.Destination.Table.GetTableFullName(),
			DestinationColumns = datasource.Destination.Table.Columns,
			DestinationIdColumn = datasource.Destination.Sequence.Column,
			KeyMapTable = keymap.Definition.TableFullName,
			KeyRelationTable = history.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			ProcessIdColumn = process.ProcessIdColumn,
			RelationTable = relation.Definition.TableFullName,
			ReverseTable = reverse.Definition.TableFullName,
			NumericType = Environment.DbEnvironment.NumericTypeName,
			ActionColumn = process.ActionColumn,
			ProcessDestinationIdColumn = process.DestinationIdColumn,
			ProcessDatasourceIdColumn = process.DatasourceIdColumn,
			InsertCountColumn = process.InsertCountColumn,
			KeyMapTableNameColumn = process.KeyMapTableNameColumn,
			KeyRelationTableNameColumn = process.KeyRelationTableNameColumn,
			ProcessTableName = process.Definition.GetTableFullName(),
			TransactionIdColumn = process.TransactionIdColumn,
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, DbDatasource datasource, Material result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(DbDatasource datasource)
	{
		var request = Environment.GetInsertRequestTable(datasource);
		var keyhistory = Environment.GetKeyRelationTable(datasource);

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

	private DeleteQuery CreateOriginDeleteQuery(DbDatasource datasource, Material result)
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

	private SelectQuery CreateAdditionalDatasourceSelectQuery(DbDatasource datasource, Material request)
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

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, DbDatasource datasource, Material request)
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

		return sq;
	}

	private CreateTableQuery CreateAdditionalMaterialQuery(DbDatasource datasource, Material request)
	{
		return CreateAdditionalMaterialQuery(datasource, request, null);
	}

	private CreateTableQuery CreateAdditionalMaterialQuery(DbDatasource datasource, Material request, Func<SelectQuery, SelectQuery>? injector = null)
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

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}
}

[GeneratePrivateProxy(typeof(AdditionalMaterializer))]
public partial struct AdditionaMaterializerProxy;