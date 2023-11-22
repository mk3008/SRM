using InterlinkMapper;
using InterlinkMapper.Models;
using RedOrb;

namespace InterlinkMapper.Materializer;

public class MaterializeService
{
	public MaterializeService(SystemEnvironment environment, LoggingDbConnection cn)
	{
		Environment = environment;
		Connection = cn;
	}

	private LoggingDbConnection Connection { get; init; }

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public MaterializeResult Create(SelectQuery query, string materialName)
	{
		Connection.Execute(query.ToCreateTableQuery(materialName), commandTimeout: CommandTimeout);

		var rows = GetRowCount(materialName);
		var columns = query.GetSelectableItems().Select(x => x.Alias).ToList();

		return new MaterializeResult
		{
			Count = rows,
			MaterialName = materialName,
			SelectQuery = CreateSelelectQuery(materialName, columns),
		};
	}

	public MaterializeResult Move(SelectQuery query, string materialName, string originTable, List<string> keys)
	{
		// materialized.
		var result = Create(query, materialName);

		if (result.Count != 0)
		{
			DeleteOrigin(result, originTable, keys);
		}

		return result;
	}

	private void DeleteOrigin(MaterializeResult result, string originTable, List<string> keys)
	{
		// erase materialized data from oprigin.
		var sq = new SelectQuery();
		sq.AddComment("Data that has been materialized will be deleted from the original.");

		var (f, d) = sq.From(originTable).As("d");

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

		Connection.Execute(sq.ToDeleteQuery(originTable), commandTimeout: CommandTimeout);
	}

	private int GetRowCount(string tableName)
	{
		var sq = new SelectQuery();
		sq.AddComment("material table rows");
		sq.From(tableName);
		sq.Select("count(*)");
		return Connection.ExecuteScalar<int>(sq, commandTimeout: CommandTimeout);
	}

	private SelectQuery CreateSelelectQuery(string tableName, List<string> columns)
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
}
