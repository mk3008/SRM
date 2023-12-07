using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseForwardingMaterializer : IMaterializer
{
	public ReverseForwardingMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RowNumberColumnName { get; set; } = "row_num";

	public ReverseMaterial? Create(IDbConnection connection, DbDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		if (!destination.AllowReverse) throw new NotSupportedException();

		var requestMaterialQuery = CreateRequestMaterialQuery(destination);
		var request = this.CreateMaterial(connection, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, destination, request);
		var deleteRows = CleanUpMaterialRequest(connection, destination, request);

		// If all requests are deleted, there are no processing targets.
		if (request.Count == deleteRows) return null;

		var query = CreateReverseMaterialQuery(destination, request, injector);
		var reverse = this.CreateMaterial(connection, query);

		return ToReverseMaterial(destination, reverse);
	}

	private ReverseMaterial ToReverseMaterial(DbDestination destination, Material material)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(destination);
		var reverse = Environment.GetReverseTable(destination);

		return new ReverseMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			RootIdColumn = reverse.RootIdColumn,
			OriginIdColumn = reverse.OriginIdColumn,
			RemarksColumn = reverse.RemarksColumn,
			DestinationTable = destination.Table.GetTableFullName(),
			DestinationColumns = destination.Table.Columns,
			DestinationIdColumn = destination.Sequence.Column,
			KeymapTableNameColumn = process.KeymapTableNameColumn,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			ProcessIdColumn = process.ProcessIdColumn,
			RelationTable = relation.Definition.TableFullName,
			ReverseTable = reverse.Definition.TableFullName,
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, DbDestination destination, Material result)
	{
		var query = CreateOriginDeleteQuery(destination, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int CleanUpMaterialRequest(IDbConnection connection, DbDestination destination, Material result)
	{
		var query = CleanUpMaterialRequestQuery(destination, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(DbDestination destination)
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

	private DeleteQuery CreateOriginDeleteQuery(DbDestination destination, Material result)
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

	private DeleteQuery CleanUpMaterialRequestQuery(DbDestination destination, Material result)
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

	private SelectQuery CreateReverseDatasourceSelectQuery(DbDestination destination, Material request)
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

	private CreateTableQuery CreateReverseMaterialQuery(DbDestination destination, Material request, Func<SelectQuery, SelectQuery>? injector)
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

		return sq.ToCreateTableQuery("__reverse_datasource");
	}
}

[GeneratePrivateProxy(typeof(ReverseForwardingMaterializer))]
public partial struct ReverseForwardingMaterializerProxy;
