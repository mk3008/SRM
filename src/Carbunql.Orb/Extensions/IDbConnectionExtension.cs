using Carbunql.Building;
using Carbunql.Dapper;
using Carbunql.Values;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb.Extensions;

public static class IDbConnectionExtension
{
	public static void CreateTableOrDefault<T>(this IDbConnection connection)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		connection.CreateTableOrDefault(def);
	}

	public static void CreateTableOrDefault(this IDbConnection connection, IDbTableDefinition tabledef)
	{
		connection.Execute(tabledef.ToCreateTableCommandText());
		foreach (var item in tabledef.ToCreateIndexCommandTexts()) connection.Execute(item);
	}

	public static T FindById<T>(this IDbConnection connection, long id, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		return connection.FindById<T>(def, id, placeholderIdentifer, Logger, timeout);
	}

	public static T FindById<T>(this IDbConnection connection, IDbTableDefinition tabledef, long id, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var sq = tabledef.ToSelectQuery();
		var pkeys = tabledef.GetPrimaryKeys();
		if (pkeys.Count != 1) throw new NotSupportedException();

		var pkey = pkeys.First();
		sq.Where(sq.FromClause!.Root, pkey.ColumnName).Equal(sq.AddParameter(placeholderIdentifer + pkey.Identifer, id));

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		return executor.Query<T>(sq).First();
	}

	public static void Insert<T>(this IDbConnection connection, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		connection.Insert(def, instance, placeholderIdentifer, Logger, timeout);
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

	public static void Update<T>(this IDbConnection connection, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		connection.Update(def, instance, placeholderIdentifer, Logger, timeout);
	}

	public static void Update<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = CreateUpdateQuery(tabledef, instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q, instance);
	}

	public static void Delete<T>(this IDbConnection connection, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		connection.Delete(def, instance, placeholderIdentifer, Logger, timeout);
	}

	public static void Delete<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = CreateDeleteQuery(tabledef, instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q);
	}

	private static (InsertQuery Query, DbColumnDefinition? Sequence) GetInsertQuery<T>(IDbTableDefinition tabledef, T instance, string placeholderIdentifer)
	{
		var seq = tabledef.GetSequenceOrDefault();

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
		var pkeys = tabledef.GetPrimaryKeys();

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
		var pkeys = tabledef.GetPrimaryKeys();

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
