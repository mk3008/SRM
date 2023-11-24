using Cysharp.Text;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Security.Cryptography;
using System.Text;

namespace InterlinkMapper.Materializer;

public class AdditionalForwardingMaterializer
{
	public AdditionalForwardingMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	//private LoggingDbConnection Connection { get; init; }

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public MaterializeResult? Create(LoggingDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var request = Environment.GetInsertRequestTable(datasource);

		var requestTable = request.Definition.TableFullName;
		var requestName = "__request";// GenerateMaterialName(requestTable);

		// materialized.
		var requestMaterial = Create(connection, request.ToSelectQuery(), requestName);

		if (requestMaterial.Count == 0) return null;

		DeleteOriginRequest(connection, requestMaterial, datasource);
		var rows = CleanUpMaterialRequest(connection, requestMaterial, datasource);
		if (requestMaterial.Count == rows) return null;

		var sq = CreateDatasourceSelectQuery(requestMaterial, datasource, injector);
		var datasourceName = "__datasource";//GenerateMaterialName(datasource.DatasourceName);
		return Create(connection, sq, datasourceName);
	}

	private MaterializeResult Create(LoggingDbConnection connection, SelectQuery query, string materialName)
	{
		connection.Execute(query.ToCreateTableQuery(materialName), commandTimeout: CommandTimeout);

		var rows = GetRowCount(connection, materialName);

		return CreateResult(materialName, rows, query);
	}

	private MaterializeResult CreateResult(string materialName, int rows, SelectQuery datasourceQuery)
	{
		var columns = datasourceQuery.GetSelectableItems().Select(x => x.Alias).ToList();
		return new MaterializeResult
		{
			Count = rows,
			MaterialName = materialName,
			SelectQuery = CreateMaterialSelelectQuery(materialName, columns),
		};
	}

	private int DeleteOriginRequest(LoggingDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CreateOriginDeleteQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int CleanUpMaterialRequest(LoggingDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CleanUpMaterialRequestQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int GetRowCount(LoggingDbConnection connection, string tableName)
	{
		var sq = GetRowCountQuery(tableName);
		return connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
	}

	private SelectQuery GetRowCountQuery(string tableName)
	{
		var sq = new SelectQuery();
		sq.AddComment("material table rows");
		sq.From(tableName);
		sq.Select("count(*)");
		return sq;
	}

	private SelectQuery CreateMaterialSelelectQuery(string tableName, List<string> columns)
	{
		var sq = new SelectQuery();
		sq.AddComment("select material table");
		var (f, d) = sq.From(tableName).As("d");
		columns.ForEach(column =>
		{
			sq.Select(d, column);
		});
		return sq;
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
			return q.ToExists();
		});

		datasourceKeys.ForEach(key => sq.Select(r, key));

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateDatasourceSelectQuery(MaterializeResult request, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(datasource.ToSelectQuery()).As("d");

		//exists (select * from REQUEST x where d.key = x.key)
		sq.Where(() =>
		{
			var requestTable = Environment.GetInsertRequestTable(datasource);

			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");

			requestTable.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(d, key);
			});

			q.SelectAll();

			return q.ToExists();
		});

		sq.Select(d);

		if (injector != null)
		{
			return injector(sq);
		}

		return sq;
	}

	/// <summary>
	/// Generate a bridge name.
	/// </summary>
	/// <param name="datasource"></param>
	/// <returns></returns>
	private string GenerateMaterialName(string name)
	{
		var sb = ZString.CreateStringBuilder();
		sb.Append("__m_");

		using MD5 md5Hash = MD5.Create();
		byte[] data = md5Hash.ComputeHash(Encoding.UTF8.GetBytes(name));

		for (int i = 0; i < 4; i++)
		{
			sb.Append(data[i].ToString("x2"));
		}
		return sb.ToString();
	}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingMaterializer))]
public partial struct MaterializeServiceProxy;