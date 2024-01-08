using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Services;

public class ValidationRequestMaterializer : IRequestMaterializer
{
	public ValidationRequestMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string MaterialName { get; set; } = "__validation_request";

	public Material Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource)
	{
		return Create(connection, transaction, datasource, null);
	}

	public Material Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var query = CreateRequestMaterialQuery(datasource, injector);
		var material = this.CreateMaterial(connection, transaction, query);

		if (material.Count == 0) return material;

		DeleteOriginRequest(connection, datasource, material);
		CleanUpRequestMaterial(connection, material, datasource);

		return material;
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
		sq.AddComment("Data that does not exist in the KeyMap is not transferred and is not subject to verification.");
		var (f, d) = sq.From(request.SelectQuery).As("d");
		var km = f.LeftJoin(keymap.Definition.TableFullName).As("keymap").On(d, keys);

		sq.Select(d, req.RequestIdColumn);
		sq.Where(km, keys.First()).IsNull();

		return sq.ToDeleteQuery(request.MaterialName);
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDatasource datasource, Material result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var request = datasource.GetValidationRequestTable(Environment);
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

		if (injector != null) sq = injector(sq);

		return sq.ToCreateTableQuery(MaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(InterlinkDatasource datasource, Material result)
	{
		var request = datasource.GetInsertRequestTable(Environment);
		var requestTable = request.Definition.TableFullName;
		var requestId = request.RequestIdColumn;

		var sq = new SelectQuery();
		sq.AddComment("data that has been materialized will be deleted from the original.");

		var (_, r) = sq.From(result.SelectQuery).As("r");

		sq.Select(r, requestId);

		return sq.ToDeleteQuery(requestTable);
	}
}

[GeneratePrivateProxy(typeof(ValidationRequestMaterializer))]
public partial struct VValidationRequestMaterializerProxy;