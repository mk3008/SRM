using RedOrb;

namespace InterlinkMapper.Models;

public class KeymapTable : IMaterialInsertable
{
	public required DbTableDefinition Definition { get; set; }

	public required string DestinationIdColumn { get; set; } = string.Empty;

	public required List<string> DatasourceKeyColumns { get; set; }
}
