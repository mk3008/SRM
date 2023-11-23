using InterlinkMapper;
using InterlinkMapper.Models;
using RedOrb;
using PrivateProxy;

namespace InterlinkMapper.Materializer;

public class MaterializeService
{
	public MaterializeService(SystemEnvironment environment)
	{
		Environment = environment;
	}

	//private LoggingDbConnection Connection { get; init; }

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public MaterializeResult Create(LoggingDbConnection connection, SelectQuery datasourceQuery, string materialName)
	{
		connection.Execute(datasourceQuery.ToCreateTableQuery(materialName), commandTimeout: CommandTimeout);

		var rows = GetRowCount(connection, materialName);

		return CreateResult(materialName, rows, datasourceQuery);
	}

	public MaterializeResult Move(LoggingDbConnection connection, SelectQuery query, string materialName, string originTable, List<string> keys)
	{
		// materialized.
		var result = Create(connection, query, materialName);

		if (result.Count != 0)
		{
			DeleteOrigin(connection, result, originTable, keys);
		}

		return result;
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


	private void DeleteOrigin(LoggingDbConnection connection, MaterializeResult result, string originTable, List<string> keys)
	{
		var query = CreateOriginDeleteQuery(result, originTable, keys);
		connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int GetRowCount(LoggingDbConnection connection, string tableName)
	{
		var sq = new SelectQuery();
		sq.AddComment("material table rows");
		sq.From(tableName);
		sq.Select("count(*)");
		return connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
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

	private DeleteQuery CreateOriginDeleteQuery(MaterializeResult result, string originTable, List<string> keys)
	{
		// erase materialized data from oprigin.
		var sq = new SelectQuery();
		sq.AddComment("Data that has been materialized will be deleted from the original.");

		var (f, d) = sq.From(originTable).As("ot");

		sq.Where(() =>
		{
			// exists (select * from REQUEST x where d.key = x.key)
			var q = new SelectQuery();
			var (_, x) = q.From(result.MaterialName).As("x");
			q.Where(() =>
			{
				ValueBase? v = null;
				keys.ForEach(key =>
				{
					var left = new ColumnValue(x, key);
					var right = new ColumnValue(d, key);
					if (v == null)
					{
						v = left;
					}
					else
					{
						v.And(left);
					}
					v.Equal(right);
				});
				if (v == null) throw new InvalidOperationException();
				return v;
			});
			q.SelectAll();
			return q.ToExists();
		});

		keys.ForEach(key => sq.Select(d, key));

		return sq.ToDeleteQuery(originTable);
	}
}

[GeneratePrivateProxy(typeof(MaterializeService))]
public partial struct MaterializeServiceProxy;