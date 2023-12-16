using InterlinkMapper.Models;
using PrivateProxy;
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

	public ReverseMaterial? Create(IDbConnection connection, InterlinkDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		if (!destination.AllowReverse) throw new NotSupportedException();

		var requestMaterialQuery = CreateRequestMaterialQuery(destination);
		var request = this.CreateMaterial(connection, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, destination, request);

		var query = CreateReverseMaterialQuery(destination, request, injector);
		var reverse = this.CreateMaterial(connection, query);

		return ToReverseMaterial(destination, reverse);
	}

	public ReverseMaterial Create(IDbConnection connection, InterlinkDestination destination, Material request)
	{
		var query = CreateReverseMaterialQuery(destination, request);
		var reverse = this.CreateMaterial(connection, query);

		return ToReverseMaterial(destination, reverse);
	}

	private ReverseMaterial ToReverseMaterial(InterlinkDestination destination, Material material)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(destination);

		return new ReverseMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			RootIdColumn = relation.RootIdColumn,
			OriginIdColumn = relation.OriginIdColumn,
			InterlinkRemarksColumn = relation.RemarksColumn,
			DestinationTable = destination.Table.GetTableFullName(),
			DestinationColumns = destination.Table.Columns,
			DestinationIdColumn = destination.Sequence.Column,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			InterlinkProcessIdColumn = process.InterlinkProcessIdColumn,
			InterlinkRelationTable = relation.Definition.TableFullName,
			DatasourceKeyColumns = null!,
			KeyRelationTable = null!,
			ActionColumn = process.ActionColumn,
			InterlinkDestinationIdColumn = process.InterlinkDestinationIdColumn,
			InterlinkDatasourceIdColumn = process.InterlinkDatasourceIdColumn,
			InsertCountColumn = process.InsertCountColumn,
			KeyMapTableNameColumn = process.KeyMapTableNameColumn,
			KeyRelationTableNameColumn = process.KeyRelationTableNameColumn,
			ProcessTableName = process.Definition.GetTableFullName(),
			InterlinkTransactionIdColumn = process.InterlinkTransactionIdColumn,
			InterlinkDatasourceId = 0!,
			InterlinkDestinationId = destination.InterlinkDestinationId
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDestination destination, Material result)
	{
		var query = CreateOriginDeleteQuery(destination, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDestination destination)
	{
		var request = Environment.GetReverseRequestTable(destination);
		var relation = Environment.GetRelationTable(destination);
		var process = Environment.GetProcessTable();

		var sq = new SelectQuery();
		sq.AddComment("Only original slips can be reversed.(where id = origin_id)");
		sq.AddComment("Only unprocessed slips can be reversed.(where reverse is null)");
		var (f, d) = sq.From(request.Definition.TableFullName).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(d, request.DestinationIdColumn);
		var reverse = f.LeftJoin(relation.Definition.TableFullName).As("reverse").On(x =>
		{
			x.Condition(r, relation.InterlinkDestinationIdColumn).Equal(x.Table, relation.OriginIdColumn);
		});
		var p = f.InnerJoin(process.Definition.TableFullName).As("p").On(r, relation.InterlinkProcessIdColumn);

		sq.Select(r, request.RequestIdColumn);
		sq.Select(r, request.DestinationIdColumn);
		sq.Select(r, relation.RootIdColumn);
		sq.Select(r, request.RemarksColumn);
		sq.Select(p, process.InterlinkDatasourceIdColumn);
		sq.Select(p, process.InterlinkDestinationIdColumn);
		sq.Select(p, process.KeyMapTableNameColumn);
		sq.Select(p, process.KeyRelationTableNameColumn);

		sq.Where(r, relation.InterlinkDestinationIdColumn).Equal(r, relation.OriginIdColumn);
		sq.Where(reverse, relation.InterlinkDestinationIdColumn).IsNull();

		return sq.ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(InterlinkDestination destination, Material result)
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

	private SelectQuery CreateReverseDatasourceSelectQuery(InterlinkDestination destination, Material request)
	{
		var relation = Environment.GetRelationTable(destination);
		var process = Environment.GetProcessTable();
		var op = destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(destination.ToSelectQuery()).As("d");
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(d, destination.Sequence.Column);

		sq.Select(rm, relation.RootIdColumn);

		sq.Select(d);

		//Rename the existing ID column and select it as the original ID
		var originIdSelectItem = sq.GetSelectableItems().Where(x => x.Alias.IsEqualNoCase(destination.Sequence.Column)).First();
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

		sq.Select(rm, process.InterlinkDatasourceIdColumn);
		sq.Select(rm, process.InterlinkDestinationIdColumn);
		sq.Select(rm, process.KeyMapTableNameColumn);
		sq.Select(rm, process.KeyRelationTableNameColumn);
		sq.Select(rm, relation.RemarksColumn);

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

		sq.Select(destination.Sequence);
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
