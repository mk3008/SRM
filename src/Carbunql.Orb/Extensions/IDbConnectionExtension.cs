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
		var iq = GetInsertQuery(tabledef, instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };

		if (iq.Sequence == null)
		{
			executor.Execute(iq.Query);
			return;
		}

		var newId = executor.ExecuteScalar<long>(iq.Query, instance);
		iq.Sequence.Identifer.ToPropertyInfo<T>().SetValue(instance, newId);
	}

	public static void Update<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = CreateUpdateQuery(tabledef, instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q, instance);
	}

	public static void Delete<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = CreateDeleteQuery(tabledef, instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q);
	}

	private static (InsertQuery Query, DbColumnDefinition? Sequence) GetInsertQuery<T>(IDbTableDefinition tabledef, T instance, string placeholderIdentifer)
	{
		var seq = tabledef.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in tabledef.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			if (item == seq && pv.Value == null) continue;
			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		var query = vq.ToSelectQuery(cols).ToInsertQuery(tabledef.GetTableFullName());

		if (seq != null) query.Returning(new ColumnValue(seq.ColumnName));

		return (query, seq);
	}

	private static UpdateQuery CreateUpdateQuery<T>(IDbTableDefinition tabledef, T instance, string placeholderIdentifer)
	{
		var pkeys = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey && !string.IsNullOrEmpty(x.Identifer)).ToList();
		if (!pkeys.Any()) throw new NotSupportedException("Primary key column not found.");

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in tabledef.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		return vq.ToSelectQuery(cols).ToUpdateQuery(tabledef.GetTableFullName(), pkeys.Select(x => x.ColumnName));
	}

	private static DeleteQuery CreateDeleteQuery<T>(IDbTableDefinition tabledef, T instance, string placeholderIdentifer)
	{
		var pkeys = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey && !string.IsNullOrEmpty(x.Identifer)).ToList();
		if (!pkeys.Any()) throw new NotSupportedException("Primary key column not found.");

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in pkeys)
		{
			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		return vq.ToSelectQuery(cols).ToDeleteQuery(tabledef.GetTableFullName());
	}
}
