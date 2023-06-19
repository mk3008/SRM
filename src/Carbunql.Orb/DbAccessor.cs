using Carbunql.Building;
using Carbunql.Orb.Extensions;
using Microsoft.Extensions.Logging;
using System.Data;

namespace Carbunql.Orb;

public class DbAccessor
{
	public ILogger? Logger { get; init; }

	public required string PlaceholderIdentifer { get; set; }

	public int Timeout { get; set; } = 60;

	public T Load<T>(IDbConnection connection, IDbTableDefinition tabledef, long? id)
	{
		if (!id.HasValue) throw new ArgumentNullException(nameof(id));
		return connection.FindById<T>(tabledef, id.Value, PlaceholderIdentifer, Logger);
	}

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
}
