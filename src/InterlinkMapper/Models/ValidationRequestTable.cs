using RedOrb;

namespace InterlinkMapper.Models;

public class ValidationRequestTable : IRequestTable
{
	public required DbTableDefinition Definition { get; set; }

	public required string RequestIdColumn { get; set; }

	public required List<string> DatasourceKeyColumns { get; set; }
}
