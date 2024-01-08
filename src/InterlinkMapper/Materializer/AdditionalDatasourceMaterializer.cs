using Carbunql.Building;
using Carbunql.Tables;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalDatasourceMaterializer : IRequestMaterializer
{
	public AdditionalDatasourceMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string MaterialName { get; set; } = "__additional_datasource";

	private string CteName { get; set; } = "additional_data";

	public AdditionalMaterial Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, Material request)
	{
		var query = CreateAdditionalMaterialQuery(datasource, request);
		var material = this.CreateMaterial(connection, transaction, query);

		return ToAdditionalMaterial(datasource, material);
	}

	private SelectQuery CreateAdditionalDatasourceSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();
		var keys = datasource.KeyColumns.Select(x => x.ColumnName);

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			cte.AddComment("inject request material filter");
			var ctef = cte.FromClause!;
			ctef.InnerJoin(request.MaterialName).As("rm").On(ctef.Root, keys);
		}

		var sq = new SelectQuery();
		sq.AddComment("inject request material filter");
		var (f, d) = sq.From(ds).As("d");
		sq.Select(d);
		f.InnerJoin(request.MaterialName).As("rm").On(d, keys);

		return sq;
	}

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, InterlinkDatasource datasource, Material request)
	{
		sq.AddComment("inject request material filter");

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
		var target = sq.With(CreateAdditionalDatasourceSelectQuery(datasource, request)).As(CteName);

		var (_, d) = sq.From(target).As("d");
		sq.Select(datasource.Destination.DbSequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(MaterialName);
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
}

[GeneratePrivateProxy(typeof(AdditionalDatasourceMaterializer))]
public partial struct AdditionalDatasourceMaterializerProxy;