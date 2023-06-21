using Dapper;

namespace Carbunql.Orb;

public static class ObjectRelationMapper
{
	private static Dictionary<Type, DbTableDefinition> Map { get; set; } = new();

	public static void AddTypeHandler<T>(DbTableDefinition<T> def)
	{
		Map.Add(typeof(T), def);
		SqlMapper.AddTypeHandler(new ObjectRelationMappableTypeHandler<T>());
	}

	public static DbTableDefinition FindFirst<T>()
	{
		return FindFirst(typeof(T));
	}

	public static DbTableDefinition FindFirst(Type type)
	{
		if (!Map.ContainsKey(type))
		{
			throw new ArgumentException(@$"'{type.FullName}' class is not registered with ObjectTableMapper..
Use the 'ObjectTableMapper.Add' method to register type and table conversion definitions.");
		}
		return Map[type];
	}
}
