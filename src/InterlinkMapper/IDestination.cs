using Carbunql.Extensions;

namespace InterlinkMapper;

public interface IDestination
{
	DbTable Table { get; set; }

	DbTableDefinition ProcessTable { get; set; }

	DbTableDefinition ValidateRequestTable { get; }

	Sequence Sequence { get; set; }

	FlipOption FlipOption { get; set; }
}

public static class DestinationExtension
{
	public static bool HasProcessTable(this IDestination source) => string.IsNullOrEmpty(source.ProcessTable.GetTableFullName()) ? false : true;

	public static bool HasFlipTable(this IDestination source) => string.IsNullOrEmpty(source.FlipOption.FlipTable.GetTableFullName()) ? false : true;

	public static bool HasValidateRequestTable(this IDestination source) => string.IsNullOrEmpty(source.ValidateRequestTable.GetTableFullName()) ? false : true;

	public static List<string> GetDifferenceCheckColumns(this IDestination source)
	{
		var q = source.Table.Columns.Where(x => !x.IsEqualNoCase(source.Sequence.Column));
		q = q.Where(x => !x.IsEqualNoCase(source.FlipOption.ExcludedColumns));
		return q.ToList();
	}
}
