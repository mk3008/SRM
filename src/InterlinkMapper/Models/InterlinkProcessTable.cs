using RedOrb;

namespace InterlinkMapper.Models;

[Obsolete]
public class InterlinkProcessTable
{
	public required DbTableDefinition Definition { get; set; }

	public required string InterlinkTransactionIdColumn { get; set; }

	public required string InterlinkProcessIdColumn { get; set; }

	public required string InterlinkDatasourceIdColumn { get; set; }

	public required string ActionNameColumn { get; set; }

	public required string InsertCountColumn { get; set; }

	public required string KeyMapTableNameColumn { get; set; }

	public required string KeyRelationTableNameColumn { get; set; }
}
