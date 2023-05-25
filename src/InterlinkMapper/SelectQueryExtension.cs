using Carbunql;
using Carbunql.Building;

namespace InterlinkMapper;

public static class SelectQueryExtension
{
	public static string Select(this SelectQuery source, string placeholderIndentifer, string parameterName, object value)
	{
		var pname = placeholderIndentifer + parameterName;
		source.Select(source.AddParameter(pname, value)).As(parameterName);
		return pname;
	}
}
