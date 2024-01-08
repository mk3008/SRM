using InterlinkMapper.Models;
using InterlinkMapper.Services;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseRequestMaterializer : IRequestMaterializer
{
	public ReverseRequestMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string MaterialName { get; set; } = "__reverse_request";

	private string RowNumberColumnName { get; set; } = "row_num";

	public Material Create(IDbConnection connection, InterlinkTransaction transaction)
	{
		return Create(connection, transaction, null);
	}

	public Material Create(IDbConnection connection, InterlinkTransaction transaction, Func<SelectQuery, SelectQuery>? injector)
	{
		var destination = transaction.InterlinkDestination;

		if (!destination.AllowReverse) throw new NotSupportedException();

		var query = CreateRequestMaterialQuery(destination, injector);
		var material = this.CreateMaterial(connection, transaction, query);

		if (material.Count == 0) return material;

		DeleteOriginRequest(connection, destination, material);
		CleanUpRequestMaterial(connection, material, destination);

		return material;
	}

	private void CleanUpRequestMaterial(IDbConnection connection, Material request, InterlinkDestination destination)
	{
		var query = CreateCleanUpRequestMaterialQuery(request, destination);
		connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private DeleteQuery CreateCleanUpRequestMaterialQuery(Material material, InterlinkDestination destination)
	{
		var request = destination.GetReverseRequestTable(Environment);
		var relation = destination.GetInterlinkRelationTable(Environment);

		var sq = new SelectQuery();
		sq.AddComment("Exclude irreversible data.");
		var (f, d) = sq.From(CreateCleanUpRequestMaterialQuery_SubQuery(material, destination)).As("d");

		sq.Select(d, request.RequestIdColumn);

		sq.Where(() =>
		{
			var v = new ColumnValue(d, RowNumberColumnName).Equal("1");
			v.And(d, relation.OriginIdColumn).Equal(d, relation.DestinationIdColumn);
			return new NegativeValue(v.ToGroup());
		});

		return sq.ToDeleteQuery(material.MaterialName);
	}

	private SelectQuery CreateCleanUpRequestMaterialQuery_SubQuery(Material material, InterlinkDestination destination)
	{
		var request = destination.GetReverseRequestTable(Environment);
		var relation = destination.GetInterlinkRelationTable(Environment);

		var sq = new SelectQuery();
		var (f, d) = sq.From(material.SelectQuery).As("d");

		sq.Select(d, request.RequestIdColumn);
		sq.Select(d, relation.OriginIdColumn);
		sq.Select(d, relation.DestinationIdColumn);

		sq.Select(new FunctionValue("row_number", () =>
		{
			var value = new OverClause();
			value.AddPartition(new ColumnValue(d, relation.RootIdColumn));
			value.AddOrder(new SortableItem(new ColumnValue(d, relation.DestinationIdColumn), isAscending: false));
			return value;
		})).As(RowNumberColumnName);

		return sq;
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDestination destination, Material result)
	{
		var query = CreateOriginDeleteQuery(destination, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDestination destination, Func<SelectQuery, SelectQuery>? injector)
	{
		var request = destination.GetReverseRequestTable(Environment);
		var relation = destination.GetInterlinkRelationTable(Environment);
		var process = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();

		var sq = new SelectQuery();
		var (f, d) = sq.From(request.Definition.TableFullName).As("req");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("rel").On(d, request.DestinationIdColumn);
		var p = f.InnerJoin(process.TableFullName).As("proc").On(r, relation.InterlinkProcessIdColumn);

		sq.Select(d, request.RequestIdColumn);

		sq.Select(r, request.DestinationIdColumn);
		sq.Select(r, relation.RootIdColumn);
		sq.Select(r, relation.OriginIdColumn);

		sq.Select(p, source.GetSequence().ColumnName);

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(MaterialName);
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
}

[GeneratePrivateProxy(typeof(ReverseRequestMaterializer))]
public partial struct ReverseRequestMaterializerProxy;
