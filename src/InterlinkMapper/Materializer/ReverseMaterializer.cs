using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseMaterializer : IMaterializer
{
	public ReverseMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RequestMaterialName { get; set; } = "__reverse_request";

	public string DatasourceMaterialName { get; set; } = "__reverse_datasource";

	public ReverseMaterial? Create(IDbConnection connection, InterlinkTransaction transaction, Func<SelectQuery, SelectQuery>? injector)
	{
		var destination = transaction.InterlinkDestination;

		if (!destination.AllowReverse) throw new NotSupportedException();

		var requestMaterialQuery = CreateRequestMaterialQuery(destination);
		var request = this.CreateMaterial(connection, transaction, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, destination, request);

		var query = CreateReverseMaterialQuery(destination, request, injector);
		var reverse = this.CreateMaterial(connection, transaction, query);

		return ToReverseMaterial(reverse);
	}

	public ReverseMaterial Create(IDbConnection connection, InterlinkTransaction transaction, Material request)
	{
		var destination = transaction.InterlinkDestination;

		var query = CreateReverseMaterialQuery(destination, request);
		var reverse = this.CreateMaterial(connection, transaction, query);

		return ToReverseMaterial(reverse);
	}

	private ReverseMaterial ToReverseMaterial(Material material)
	{
		var transaction = material.InterlinkTransaction;

		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var sourceId = source.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkDatasource.InterlinkDatasourceId)).First();

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var relation = transaction.InterlinkDestination.GetInterlinkRelationTable(Environment);

		var destination = transaction.InterlinkDestination;

		return new ReverseMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			RootIdColumn = relation.RootIdColumn,
			OriginIdColumn = relation.OriginIdColumn,
			InterlinkRemarksColumn = relation.RemarksColumn,
			DestinationTable = destination.DbTable.TableFullName,
			DestinationColumns = destination.DbTable.ColumnNames,
			DestinationSeqColumn = destination.DbSequence.ColumnName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkRelationTable = relation.Definition.TableFullName,
			//DatasourceKeyColumns = source.GetPrimaryKeys().Select(x => x.ColumnName).ToList(),
			//KeyRelationTable = null!,
			//ActionColumn = process.ActionNameColumn,
			InterlinkDatasourceIdColumn = sourceId.ColumnName,
			//InsertCountColumn = process.InsertCountColumn,
			//KeyMapTableNameColumn = process.KeyMapTableNameColumn,
			//KeyRelationTableNameColumn = process.KeyRelationTableNameColumn,
			//ProcessTableName = process.Definition.TableFullName,
			//InterlinkTransactionIdColumn = process.InterlinkTransactionIdColumn,
			//InterlinkDatasourceId = 0!,
			//InterlinkDestinationId = destination.InterlinkDestinationId,
			InterlinkTransaction = transaction,
			Environment = Environment
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDestination destination, Material result)
	{
		var query = CreateOriginDeleteQuery(destination, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDestination destination)
	{
		var request = destination.GetReverseRequestTable(Environment);
		var relation = destination.GetInterlinkRelationTable(Environment);
		var process = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();

		var sq = new SelectQuery();
		sq.AddComment("Only original slips can be reversed.(where id = origin_id)");
		sq.AddComment("Only unprocessed slips can be reversed.(where reverse is null)");
		var (f, d) = sq.From(request.Definition.TableFullName).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(d, request.DestinationIdColumn);
		var rev = f.LeftJoin(relation.Definition.TableFullName).As("rev").On(x =>
		{
			x.Condition(r, relation.InterlinkDestinationIdColumn).Equal(x.Table, relation.OriginIdColumn);
			x.Condition(x.Table, relation.InterlinkDestinationIdColumn).NotEqual(x.Table, relation.RootIdColumn);
		});
		var p = f.InnerJoin(process.TableFullName).As("p").On(r, relation.InterlinkProcessIdColumn);

		sq.Select(d, request.RequestIdColumn);
		sq.Select(r, request.DestinationIdColumn);
		sq.Select(r, relation.RootIdColumn);
		sq.Select(p, source.GetSequence().ColumnName);

		sq.Where(r, relation.InterlinkDestinationIdColumn).Equal(r, relation.OriginIdColumn);
		sq.Where(rev, relation.InterlinkDestinationIdColumn).IsNull();

		return sq.ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(InterlinkDestination destination, Material result)
	{
		var request = destination.GetReverseRequestTable(Environment);
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

	private SelectQuery CreateReverseDatasourceSelectQuery(InterlinkDestination destination, Material request)
	{
		var relation = destination.GetInterlinkRelationTable(Environment);
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var op = destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(destination.ToSelectQuery()).As("d");
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(d, destination.DbSequence.ColumnName);

		sq.Select(rm, relation.RootIdColumn);

		sq.Select(d);

		//Rename the existing ID column and select it as the original ID
		var originIdSelectItem = sq.GetSelectableItems().Where(x => x.Alias.IsEqualNoCase(destination.DbSequence.ColumnName)).First();
		originIdSelectItem.SetAlias(relation.OriginIdColumn);

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

		sq.Select(rm, source.GetSequence().ColumnName);
		sq.Select("'force'").As(relation.RemarksColumn);//interlink remarks

		return sq;
	}

	private CreateTableQuery CreateReverseMaterialQuery(InterlinkDestination destination, Material request)
	{
		return CreateReverseMaterialQuery(destination, request, null);
	}

	private CreateTableQuery CreateReverseMaterialQuery(InterlinkDestination destination, Material request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateReverseDatasourceSelectQuery(destination, request)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");

		sq.Select(destination.DbSequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}
}

[GeneratePrivateProxy(typeof(ReverseMaterializer))]
public partial struct ReverseMaterializerProxy;
