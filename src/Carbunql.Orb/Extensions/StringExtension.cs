using System.Reflection;

namespace Carbunql.Orb.Extensions;

internal static class StringExtension
{
	public static PropertyInfo ToPropertyInfo<T>(this string identifer)
	{
		var prop = typeof(T).GetProperty(identifer);
		if (prop == null) throw new InvalidOperationException($"Failed to get Property from Identifer. Type:{typeof(T).FullName}, Identifer:{identifer}");
		return prop;
	}
}
