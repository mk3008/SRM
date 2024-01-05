using InterlinkMapper.Models;
using RedOrb;

namespace InterlinkMapper;

public static class SelectQueryExtension
{
	public static string Select(this SelectQuery source, DbEnvironment config, string column, object value)
	{
		var pname = config.PlaceHolderIdentifer + column;
		source.Select(source.AddParameter(pname, value)).As(column);
		return pname;
	}

	public static string Select(this SelectQuery source, string placeHolderIdentifer, string column, object value)
	{
		var pname = placeHolderIdentifer + column;
		source.Select(source.AddParameter(pname, value)).As(column);
		return pname;
	}

	public static void Select(this SelectQuery source, Sequence sequence)
	{
		source.Select(sequence.CommandText).As(sequence.ColumnName);
	}
}
