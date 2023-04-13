using Carbunql.Extensions;

namespace InterlinkMapper.Data;

public class Destination
{
	public int DestinationId { get; set; }

	public DbTable Table { get; set; } = new();

	public DbTable ProcessMap { get; set; } = new();

	public Sequence Sequence { get; set; } = new();

	public string Description { get; set; } = string.Empty;

	public ReverseOption ReverseOption { get; set; } = new();

	public List<string> GetDifferenceCheckColumns()
	{
		var q = Table.Columns.Where(x => !x.IsEqualNoCase(Sequence.Column));
		q = q.Where(x => !x.IsEqualNoCase(ReverseOption.ExcludedColumns));
		return q.ToList();
	}
}