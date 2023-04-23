using Carbunql.Analysis.Parser;
using Carbunql.Extensions;
using Cysharp.Text;
using Dapper;
using InterlinkMapper.Data;
using Microsoft.Extensions.Logging;
using System.Data;

namespace InterlinkMapper.Services;

public class DbEnvironmentService
{
	public DbEnvironmentService(IDbConnection cn, ILogger? logger = null)
	{
		Connection = cn;
		Logger = logger;
	}

	private readonly ILogger? Logger;

	private IDbConnection Connection { get; init; }


	public void CreateTableOrDefault(DbTableDefinition def)
	{
		var sql = def.ToCreateCommandText();
		Logger?.LogInformation("create table sql : {Sql}", sql);

		Connection.Execute(sql);
	}

	//public string NumericType { get; init; }

	//	public void CreateProcessTableOrDefault(ProcessTable table)
	//	{
	//		table.
	//		var pkeys = new[] { ds.Destination.Sequence.Column };
	//		var ukeys = Enumerable.Empty<string>();
	//		var types = new Dictionary<string, string>();

	//		ds.ProcessMapTable.Columns.ForEach(x =>
	//		{
	//			if (x.IsEqualNoCase(ds.Destination.Sequence.Column))
	//			{
	//				types.Add(x, NumericType);
	//				return;
	//			}
	//			if (columnTypes.ContainsKey(x))
	//			{
	//				types.Add(x, columnTypes[x]);
	//				return;
	//			}
	//			else
	//			{
	//				types.Add(x, NumericType);
	//			}
	//		});

	//		foreach (var key in pkeys) { }

	//		CreateTableOrDefault(table, types, pkeyColumns, Enumerable.Empty<string>());
	//	}

	//	public void CreateProcessMapTableOrDefault(Datasource ds, Dictionary<string, string> columnTypes)
	//	{
	//		var pkeys = new[] { ds.Destination.Sequence.Column };
	//		var ukeys = Enumerable.Empty<string>();
	//		var types = new Dictionary<string, string>();

	//		ds.ProcessMapTable.Columns.ForEach(x =>
	//		{
	//			if (x.IsEqualNoCase(ds.Destination.Sequence.Column))
	//			{
	//				types.Add(x, NumericType);
	//				return;
	//			}
	//			if (columnTypes.ContainsKey(x))
	//			{
	//				types.Add(x, columnTypes[x]);
	//				return;
	//			}
	//			else
	//			{
	//				types.Add(x, NumericType);
	//			}
	//		});

	//		foreach (var key in pkeys) { }

	//		CreateTableOrDefault(ds.ProcessMapTable, types, pkeys, ukeys);
	//		CreateIndexOrDefault(ds.ProcessMapTable, 0, new[] { ds.Destination.Sequence.Column });
	//	}

	//	public void CreateKeyMapTableOrDefault(Datasource ds, Dictionary<string, string> columnTypes)
	//	{
	//		var pkeys = new[] { ds.Destination.Sequence.Column };
	//		var ukeys = ds.KeyColumns;
	//		var types = new Dictionary<string, string>();

	//		ds.KeyMapTable.Columns.ForEach(x =>
	//		{
	//			if (x.IsEqualNoCase(ds.Destination.Sequence.Column))
	//			{
	//				types.Add(x, NumericType);
	//				return;
	//			}
	//			if (columnTypes.ContainsKey(x))
	//			{
	//				types.Add(x, columnTypes[x]);
	//				return;
	//			}
	//			else
	//			{
	//				types.Add(x, NumericType);
	//			}
	//		});

	//		foreach (var key in pkeys) { }

	//		CreateTableOrDefault(ds.KeyMapTable, types, pkeys, ukeys);
	//	}

	//	public void CreateRelationMapTableOrDefault(Datasource ds, Dictionary<string, string> columnTypes)
	//	{
	//		var pkeys = new[] { ds.Destination.Sequence.Column };
	//		var ukeys = Enumerable.Empty<string>();
	//		var types = new Dictionary<string, string>();

	//		ds.RelationMapTable.Columns.ForEach(x =>
	//		{
	//			if (x.IsEqualNoCase(ds.Destination.Sequence.Column))
	//			{
	//				types.Add(x, NumericType);
	//				return;
	//			}
	//			if (columnTypes.ContainsKey(x))
	//			{
	//				types.Add(x, columnTypes[x]);
	//				return;
	//			}
	//			else
	//			{
	//				types.Add(x, NumericType);
	//			}
	//		});

	//		foreach (var key in pkeys) { }

	//		CreateTableOrDefault(ds.RelationMapTable, types, pkeys, ukeys);
	//		CreateIndexOrDefault(ds.RelationMapTable, 0, ds.KeyColumns);
	//	}

	//	private void CreateTableOrDefault(DbTable tbl, Dictionary<string, string> columnTypes, IEnumerable<string> pkeyColumns, IEnumerable<string> ukeyColumns)
	//	{
	//		var tableName = ValueParser.Parse(tbl.TableFullName).ToText();

	//		var sb = ZString.CreateStringBuilder();
	//		foreach (var column in tbl.Columns.Select(x => ValueParser.Parse(x).ToText()))
	//		{
	//			var tp = ValueParser.Parse(columnTypes[column]).ToText();
	//			if (sb.Length > 0) sb.AppendLine(", ");
	//			sb.Append(column + " " + tp + " not null");
	//		}

	//		if (pkeyColumns.Any())
	//		{
	//			var columnText = string.Join(", ", pkeyColumns.Select(x => ValueParser.Parse(x).ToText()));
	//			if (sb.Length > 0) sb.AppendLine(", ");
	//			sb.Append("primary key(" + string.Join(", ", columnText) + ")");
	//		}
	//		if (ukeyColumns.Any())
	//		{
	//			var columnText = string.Join(", ", ukeyColumns.Select(x => ValueParser.Parse(x).ToText()));
	//			if (sb.Length > 0) sb.AppendLine(", ");
	//			sb.Append("unique(" + string.Join(", ", columnText) + ")");
	//		}

	//		var sql = @$"create table if not exists {tableName} (
	//{sb}
	//)";
	//		Connection.Execute(sql);
	//	}

	//	private void CreateIndexOrDefault(DbTable tbl, int number, IEnumerable<string> columns)
	//	{
	//		if (!columns.Any()) return;

	//		var indexName = $"i{number}_{ValueParser.Parse(tbl.TableName).ToText()}";

	//		var tableName = ValueParser.Parse(tbl.TableFullName).ToText();

	//		var sb = ZString.CreateStringBuilder();
	//		foreach (var column in columns.Select(x => ValueParser.Parse(x).ToText()))
	//		{
	//			if (sb.Length > 0) sb.AppendLine(", ");
	//			sb.Append(column);
	//		}

	//		var sql = @$"create index {indexName} if not exists on {tableName} (
	//{sb}
	//)";
	//		Connection.Execute(sql);
	//	}
}
