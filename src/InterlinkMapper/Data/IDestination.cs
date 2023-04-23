using Carbunql.Extensions;

namespace InterlinkMapper.Data;

public interface IDestination
{
	DbTable Table { get; set; }

	DbTable ProcessMap { get; set; }

	Sequence Sequence { get; set; }

	ReverseOption ReverseOption { get; set; }
}

public static class DestinationExtension
{
	public static List<string> GetDifferenceCheckColumns(this IDestination source)
	{
		var q = source.Table.Columns.Where(x => !x.IsEqualNoCase(source.Sequence.Column));
		q = q.Where(x => !x.IsEqualNoCase(source.ReverseOption.ExcludedColumns));
		return q.ToList();
	}
}
