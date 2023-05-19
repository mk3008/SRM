using Carbunql;
using Carbunql.Building;

namespace InterlinkMapper;

public static class SelectQueryExtension
{
	public static void Select(this SelectQuery source, string placeholderIndentifer, string parameterName, object value)
	{
		source.Select(source.AddParameter(placeholderIndentifer + parameterName, value)).As(parameterName);
	}
}
