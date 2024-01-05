using RedOrb;
using RedOrb.Attributes;

namespace InterlinkMapper.Models;

[DbTable]
public class InterlinkProcess
{
	[DbColumn("numeric", IsAutoNumber = true, IsPrimaryKey = true)]
	public long InterlinkProcessId { get; set; }

	[DbParentRelationColumn("numeric", nameof(InterlinkTransaction.InterlinkTransactionId))]
	public required InterlinkTransaction InterlinkTransaction { get; set; }

	[DbParentRelationColumn("numeric", nameof(InterlinkDatasource.InterlinkDatasourceId))]
	public required InterlinkDatasource InterlinkDatasource { get; set; }

	[DbColumn("text")]
	public required string ActionName { get; set; }

	[DbColumn("numeric")]
	public long InsertCount { get; set; }

	[DbColumn("timestamp", SpecialColumn = SpecialColumn.CreateTimestamp)]
	public DateTime CreatedAt { get; set; }
}
