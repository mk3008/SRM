using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Values;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb.Extensions;

internal static class IDbConnectionExtension
{
	public static void Save<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer)
	{
		var seq = tabledef.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();
		if (seq == null) throw new NotSupportedException("AutoNumber column not found.");

		var id = seq.Identifer.ToPropertyInfo<T>().GetValue(instance);
		if (id == null)
		{
			connection.Insert(tabledef, instance, placeholderIdentifer);
			return;
		}
		connection.Update(tabledef, instance, placeholderIdentifer);
	}

	public static void Insert<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };

		var seq = tabledef.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();

		var row = new ValueCollection();
		var cols = new List<string>();
		foreach (var item in tabledef.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			if (item == seq && pv.Value == null) continue;
			cols.Add(item.ColumnName);
			row.Add(pv);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		var sq = vq.ToSelectQuery(cols);

		var q = sq.ToInsertQuery(tabledef.GetTableFullName());

		if (seq == null)
		{
			executor.Execute(q);
			return;
		}

		q.Returning(new ColumnValue(seq.ColumnName));
		var newId = executor.ExecuteScalar<long>(q, instance);
		seq.Identifer.ToPropertyInfo<T>().SetValue(instance, newId);
	}

	public static void Update<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var pkey = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey).FirstOrDefault();
		if (pkey == null) throw new NotSupportedException("Primary key column not found.");

		var row = new ValueCollection();
		foreach (var item in tabledef.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			if (item == pkey && pv.Value == null) throw new InvalidOperationException($"Primary key is null. Identifer:{item.Identifer}");
			row.Add(pv);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		var q = vq.ToUpdateQuery(tabledef.GetTableFullName(), new[] { pkey.Identifer });
		connection.Execute(q);
	}

	public static void Delete<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };

		var pkey = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey).FirstOrDefault();
		if (pkey == null) throw new NotSupportedException("Primary key column not found.");

		var row = new ValueCollection();
		var cols = new List<string>();
		foreach (var item in tabledef.ColumnDefinitions)
		{
			if (item != pkey) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			if (pv.Value == null) throw new InvalidOperationException($"Primary key is null. Identifer:{item.Identifer}");
			cols.Add(item.ColumnName);
			row.Add(pv);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		var q = vq.ToSelectQuery(cols).ToDeleteQuery(tabledef.GetTableFullName());
		executor.Execute(q);
	}
}
