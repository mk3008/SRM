using Carbunql.Building;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb.Extensions;

public static class IDbConnectionExtension
{
	public static void CreateTableOrDefault<T>(this IDbConnection connection)
	{
		var def = ObjectRelationMapper.FindFirst<T>();
		connection.CreateTableOrDefault(def);
	}

	public static void CreateTableOrDefault(this IDbConnection connection, IDbTableDefinition tabledef)
	{
		connection.Execute(tabledef.ToCreateTableCommandText());
		foreach (var item in tabledef.ToCreateIndexCommandTexts()) connection.Execute(item);
	}

	public static T FindById<T>(this IDbConnection connection, long id, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectRelationMapper.FindFirst<T>();
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

	//public static T1 FindById<T1, T2>(this IDbConnection connection, IDbTableDefinition tabledef, long id, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	//{
	//	var def1 = ObjectTableMapper.FindFirst<T1>();
	//	var sq = def1.ToSelectQuery();

	//	var def2 = ObjectTableMapper.FindFirst<T2>();


	//	return connection.FindById<T>(def, id, placeholderIdentifer, Logger, timeout);
	//}

	public static void Insert<T>(this IDbConnection connection, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectRelationMapper.FindFirst<T>();
		connection.Insert(def, instance, placeholderIdentifer, Logger, timeout);
	}

	public static void Insert<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var iq = tabledef.ToInsertQuery(instance, placeholderIdentifer);

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
		var def = ObjectRelationMapper.FindFirst<T>();
		connection.Update(def, instance, placeholderIdentifer, Logger, timeout);
	}

	public static void Update<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = tabledef.ToUpdateQuery(instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q, instance);
	}

	public static void Delete<T>(this IDbConnection connection, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var def = ObjectRelationMapper.FindFirst<T>();
		connection.Delete(def, instance, placeholderIdentifer, Logger, timeout);
	}

	public static void Delete<T>(this IDbConnection connection, IDbTableDefinition tabledef, T instance, string placeholderIdentifer, ILogger? Logger = null, int? timeout = null)
	{
		var q = tabledef.ToDeleteQuery(instance, placeholderIdentifer);

		var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = timeout };
		executor.Execute(q);
	}
}
