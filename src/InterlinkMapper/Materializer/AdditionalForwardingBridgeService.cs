using Cysharp.Text;
using Dapper;
using InterlinkMapper;
using InterlinkMapper.Models;
using RedOrb;
using System.Data;
using System.Security.Cryptography;
using System.Text;
using PrivateProxy;

namespace InterlinkMapper.Materializer;

/// <summary>
/// A service that creates a bridge table from data sources that are not registered in the key map.
/// </summary>
public class AdditionalForwardingBridgeService
{
	public AdditionalForwardingBridgeService(SystemEnvironment environment)
	{
		Environment = environment;
		MaterializeService = new MaterializeService(environment);
	}

	private SystemEnvironment Environment { get; init; }

	private MaterializeService MaterializeService { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

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

	private MaterializeResult CreateRequestMaterial(LoggingDbConnection connection, DbDatasource datasource)
	{
		var request = Environment.GetInsertRequestTable(datasource);
		var table = request.Definition.TableFullName;
		var keys = request.Definition.Columns.ToList();

		var sq = new SelectQuery();
		sq.From(table);
		keys.Select(key => sq.Select(key));

		var name = GenerateMaterialName(request.Definition.TableFullName);
		return MaterializeService.Move(connection, sq, name, table, keys);
	}

	/// <summary>
	/// Write requested data source to bridge table.
	/// ex.
	/// create table BRIDGE 
	/// as
	/// select d.*, r.id from DATASOURCE d inner join REQUEST r on d.key = r.key
	/// </summary>
	/// <param name="datasource"></param>
	/// <returns></returns>
	/// <exception cref="InvalidOperationException"></exception>
	public MaterializeResult Create(LoggingDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var request = CreateRequestMaterial(connection, datasource);

		CleanUpRequestMaterial(connection, datasource, request);

		return Create(connection, datasource, request, injector);
	}

	private MaterializeResult Create(LoggingDbConnection connection, DbDatasource datasource, MaterializeResult request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(CreateDatasourceSelectQuery(datasource, request, injector)).As("d");

		sq.Select(d);
		sq.Select(datasource.Destination.Sequence);

		var bridgeName = GenerateMaterialName(datasource.DatasourceName);

		return MaterializeService.Create(connection, sq, bridgeName);
	}

	private SelectQuery CreateDatasourceSelectQuery(DbDatasource datasource, MaterializeResult request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		sq.AddComment("Data source to be added");
		var (f, d) = sq.From(datasource.ToSelectQuery()).As("d");

		//exists (select * from REQUEST x where d.key = x.key)
		sq.Where(() =>
		{
			var requestTable = Environment.GetInsertRequestTable(datasource);

			var q = new SelectQuery();
			q.AddComment("exists request");

			var (_, x) = q.From(request.MaterialName).As("x");

			requestTable.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(d, key);
			});

			q.SelectAll();

			return q.ToExists();
		});

		//not exists (select * from KEYMAP x where d.key = x.key)
		sq.Where(() =>
		{
			var keymapTable = Environment.GetKeymapTable(datasource);

			var q = new SelectQuery();
			q.AddComment("not exists keymap");

			var (_, x) = q.From(keymapTable.Definition.TableFullName).As("x");

			keymapTable.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(d, key);
			});

			q.SelectAll();

			return q.ToNotExists();
		});

		sq.Select(d);

		if (injector != null)
		{
			return injector(sq);
		}

		return sq;
	}

	/// <summary>
	/// Transferred keys will be removed from additional requests.
	/// ex. delete from REQUEST d where exists (select * from KEYMAP x where x.key = d.key)
	/// </summary>
	/// <param name="datasource"></param>
	public void CleanUpRequestMaterial(LoggingDbConnection connection, DbDatasource datasource, MaterializeResult request)
	{
		var deleteQuery = CreateRequestMaterialCleanUpQuery(datasource, request);
		connection.Execute(deleteQuery, commandTimeout: CommandTimeout);
	}

	public DeleteQuery CreateRequestMaterialCleanUpQuery(DbDatasource datasource, MaterializeResult request)
	{
		var requestTable = Environment.GetInsertRequestTable(datasource);
		var keymapTable = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("Delete transferred keys");
		var (f, d) = sq.From(request.MaterialName).As("d");

		sq.Where(() =>
		{
			// exists keymap
			var q = new SelectQuery();
			q.AddComment("exists keymap");

			var (_, x) = q.From(keymapTable.Definition.TableFullName).As("x");

			keymapTable.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(d, key);
			});

			q.SelectAll();

			return q.ToExists();
		});

		sq.Select(requestTable.RequestIdColumn);

		return sq.ToDeleteQuery(request.MaterialName);
	}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingBridgeService))]
public partial struct AdditionalForwardingBridgeServiceProxy;