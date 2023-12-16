using RedOrb;

namespace InterlinkMapper.Models;

public class ReverseRequestTable : IRequestTable
{
	public required DbTableDefinition Definition { get; set; }

	public required string RequestIdColumn { get; set; }

	public required string DestinationIdColumn { get; set; }

	public required string RemarksColumn { get; set; }
}
