namespace Carbunql.Orb;

public static class ObjectTableMapper
{
	private static Dictionary<Type, DbTableDefinition> Map { get; set; } = new();

	public static void Add(MappableDbTableDefinition def)
	{
		Map.Add(def.Type, def);
	}

	public static void Add(Type type, DbTableDefinition def)
	{
		Map.Add(type, def);
	}

	public static DbTableDefinition FindFirst<T>()
	{
		return FindFirst(typeof(T));
	}

	public static DbTableDefinition FindFirst(Type type)
	{
		if (!Map.ContainsKey(type))
		{
			throw new ArgumentException(@$"Not registered with ObjectTableMapper.
Use the 'ObjectTableMapper.Add' method to register type and table conversion definitions.(type:{type.FullName})");
		}
		return Map[type];
	}
}
