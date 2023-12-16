using InterlinkMapper.Models;

namespace InterlinkMapper.Test;

internal static class DestinationRepository
{
	public static InterlinkDestination sale_journals => new InterlinkDestination()
	{
		InterlinkDestinationId = 2,
		Table = new()
		{
			TableName = "sale_journals",
			Columns = new() {
						"sale_journal_id",
						"journal_closing_date",
						"sale_date",
						"shop_id",
						"price",
						"remarks"
					}
		},
		Sequence = new()
		{
			Column = "sale_journal_id",
			Command = "nextval('sale_journals_sale_journal_id_seq'::regclass)"
		},
		ReverseOption = new()
		{
			ExcludedColumns = ["remarks"],
			ReverseColumns = ["price"]
		}

	};
}
