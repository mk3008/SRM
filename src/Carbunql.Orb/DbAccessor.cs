using Carbunql.Orb.Extensions;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb;

public class DbAccessor
{
	public ILogger? Logger { get; init; }

	public required string PlaceholderIdentifer { get; set; }

	public int Timeout { get; set; } = 60;

	//public T FindById<T>(IDbConnection connection, long? id)
	//{
	//	if (!id.HasValue) throw new ArgumentNullException(nameof(id));
	//	return connection.FindById<T>(id.Value, PlaceholderIdentifer, Logger, Timeout);
	//}

	//public T1 Load<T1, T2>(IDbConnection connection, long? id)
	//{
	//	if (!id.HasValue) throw new ArgumentNullException(nameof(id));
	//	return connection.FindById<T1, T2>(id.Value, PlaceholderIdentifer, Logger, Timeout);
	//}

	public void Save<T>(IDbConnection connection, T instance)
	{
		var def = ObjectRelationMapper.FindFirst<T>();

		var seq = def.GetSequenceOrDefault();
		if (seq == null) throw new NotSupportedException("AutoNumber column not found.");

		var id = seq.Identifer.ToPropertyInfo<T>().GetValue(instance);
		if (id == null)
		{
			connection.Insert(def, instance, PlaceholderIdentifer, Logger, Timeout);
			return;
		}
		connection.Update(def, instance, PlaceholderIdentifer, Logger, Timeout);
	}

	public void Delete<T>(IDbConnection connection, T instance)
	{
		connection.Delete(instance, PlaceholderIdentifer, Logger, Timeout);
	}
}
