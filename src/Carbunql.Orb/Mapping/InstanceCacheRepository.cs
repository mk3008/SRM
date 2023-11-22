namespace Carbunql.Orb.Mapping;

internal class InstanceCacheRepository
{
	private Dictionary<(Type, string), object> Instances { get; set; } = new();

	public object? LoadOrDefault(Type tp, string key)
	{
		if (Instances.TryGetValue((tp, key), out var instance))
		{
			return instance;
		}
		return null;
	}

	public void Save(Type tp, string key, object instance)
	{
		Instances.Add((tp, key), instance);
	}
}