using Carbunql.Building;
using Carbunql.Orb.Extensions;
using Carbunql.Values;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb;

public class DbAccessor
{
	public ILogger? Logger { get; init; }

	public required string PlaceholderIdentifer { get; set; }

	public int Timeout { get; set; } = 60;

	public void Save<T>(IDbConnection connection, IDbTableDefinition tabledef, T instance)
	{
		var seq = tabledef.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();
		if (seq == null) throw new NotSupportedException("AutoNumber column not found.");

		var id = seq.Identifer.ToPropertyInfo<T>().GetValue(instance);
		if (id == null)
		{
			connection.Insert(tabledef, instance, PlaceholderIdentifer, Logger);
			return;
		}
		connection.Update(tabledef, instance, PlaceholderIdentifer, Logger);
	}

	public void Delete<T>(IDbConnection connection, IDbTableDefinition tabledef, T instance)
	{
		connection.Delete(tabledef, instance, PlaceholderIdentifer, Logger);
	}

	//public void Insert<T>(IDbConnection connection, IDbTableDefinition tabledef, T instance)
	//{
	//	var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = Timeout };

	//	var seq = tabledef.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();

	//	var row = new ValueCollection();
	//	foreach (var item in tabledef.ColumnDefinitions)
	//	{
	//		if (string.IsNullOrEmpty(item.Identifer)) continue;


	//		var prop = item.Identifer.ToPropertyInfo<T>();
	//		var pv = prop.ToParameterValue(instance, PlaceholderIdentifer);

	//		if (item == seq && pv.Value == null) continue;
	//		row.Add(pv);
	//	}

	//	var vq = new ValuesQuery(new List<ValueCollection>() { row });
	//	var q = vq.ToInsertQuery(tabledef.GetTableFullName());

	//	if (seq == null)
	//	{
	//		executor.Execute(q);
	//		return;
	//	}
	//	q.Returning(new ColumnValue(seq.ColumnName));
	//	var newId = executor.ExecuteScalar<long>(q);
	//	seq.Identifer.ToPropertyInfo<T>().SetValue(instance, newId);
	//}

	//public void Update<T>(IDbConnection connection, IDbTableDefinition tabledef, T instance)
	//{
	//	var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = Timeout };

	//	var pkey = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey).FirstOrDefault();
	//	if (pkey == null) throw new NotSupportedException("Primary key column not found.");

	//	var row = new ValueCollection();
	//	foreach (var item in tabledef.ColumnDefinitions)
	//	{
	//		if (string.IsNullOrEmpty(item.Identifer)) continue;

	//		var prop = item.Identifer.ToPropertyInfo<T>();
	//		var pv = prop.ToParameterValue(instance, PlaceholderIdentifer);

	//		if (item == pkey && pv.Value == null) throw new InvalidOperationException($"Primary key is null. Identifer:{item.Identifer}");
	//		row.Add(pv);
	//	}

	//	var vq = new ValuesQuery(new List<ValueCollection>() { row });
	//	var q = vq.ToUpdateQuery(tabledef.GetTableFullName(), new[] { pkey.Identifer });
	//	executor.Execute(q);
	//}

	//public void Delete<T>(IDbConnection connection, IDbTableDefinition tabledef, T instance)
	//{
	//	var executor = new QueryExecutor() { Connection = connection, Logger = Logger, Timeout = Timeout };

	//	var pkey = tabledef.ColumnDefinitions.Where(x => x.IsPrimaryKey).FirstOrDefault();
	//	if (pkey == null) throw new NotSupportedException("Primary key column not found.");

	//	var row = new ValueCollection();
	//	foreach (var item in tabledef.ColumnDefinitions)
	//	{
	//		if (string.IsNullOrEmpty(item.Identifer)) continue;

	//		var prop = item.Identifer.ToPropertyInfo<T>();
	//		var pv = prop.ToParameterValue(instance, PlaceholderIdentifer);

	//		if (item == pkey && pv.Value == null) throw new InvalidOperationException($"Primary key is null. Identifer:{item.Identifer}");
	//		row.Add(pv);
	//	}

	//	var vq = new ValuesQuery(new List<ValueCollection>() { row });
	//	var q = vq.ToUpdateQuery(tabledef.GetTableFullName(), new[] { pkey.Identifer });
	//	executor.Execute(q);
	//}
}
