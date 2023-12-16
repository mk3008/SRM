using RedOrb;

namespace InterlinkMapper.Models;

public class ReverseTable : IMaterialInsertable
{
	public required DbTableDefinition Definition { get; set; }

	public required string RootIdColumn { get; set; }

	public required string OriginIdColumn { get; set; }

	public required string ReverseIdColumn { get; set; }

	public required string RemarksColumn { get; set; }
}
