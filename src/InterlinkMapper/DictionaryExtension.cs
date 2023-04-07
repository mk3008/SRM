using Carbunql;
using Carbunql.Building;

public static class DictionaryExtension
{
	public static SelectQuery ToSelectQuery(this Dictionary<string, object> source, string placeholderIdentifier)
	{
		var sq = new SelectQuery();
		foreach (var item in source)
		{
			var pname = placeholderIdentifier + item.Key;
			sq.Select(pname).As(item.Key);
			sq.Parameters.Add(pname, item.Value);
		}
		return sq;
	}
}