using InterlinkMapper.Services;
using RedOrb;
using RedOrb.Attributes;

namespace InterlinkMapper.Models;

[DbTable]
public class InterlinkTransaction
{
	[DbColumn("numeric", IsAutoNumber = true, IsPrimaryKey = true)]
	public long InterlinkTransactionId { get; set; }

	[DbParentRelationColumn("numeric", nameof(InterlinkDestination.InterlinkDestinationId))]
	public required InterlinkDestination InterlinkDestination { get; set; }

	[DbColumn("text")]
	public required string ServiceName { get; set; }

	[DbColumn("text")]
	public required string Argument { get; set; } = string.Empty;

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.CreateTimestamp)]
	public DateTime CreatedAt { get; set; }
}
