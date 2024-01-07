using InterlinkMapper.Models;

namespace PostgresSample;

internal static class DestinationRepository
{
	public static IEnumerable<InterlinkDestination> GetAll()
	{
		yield return Customers;
		yield return SaleJournals;
	}

	public static InterlinkDestination Customers => Getustomers();

	private static InterlinkDestination Getustomers()
	{
		var destination = new InterlinkDestination()
		{
			TableFullName = "customer",
			DbTable = new()
			{
				TableName = "customer",
				ColumnNames = new() {
				"customer_id",
				"customer_type",
				"customer_name",
			}
			},
			DbSequence = new()
			{
				ColumnName = "customer_id",
				CommandText = "nextval('customer_customer_id_seq'::regclass)"
			},
			ReverseOption = new()
			{
				ExcludedColumns = ["customer_type"],
				ReverseColumns = new()
			},
			Description = ""
		};

		destination.Datasources.Add(DatasourceRepository.NetMembers);
		destination.Datasources.Add(DatasourceRepository.CorporateCustomers);

		return destination;
	}

	public static InterlinkDestination SaleJournals => GetSaleJournals();

	private static InterlinkDestination GetSaleJournals()
	{
		var destination = new InterlinkDestination
		{
			TableFullName = "sale_journal",
			DbTable = new()
			{
				TableName = "sale_journal",
				ColumnNames = new() {
				"sale_journal_id",
				"sale_date",
				"price",
			}
			},
			DbSequence = new()
			{
				ColumnName = "sale_journal_id",
				CommandText = "nextval('sale_journal_sale_journal_id_seq'::regclass)"
			},
			ReverseOption = new()
			{
				ReverseColumns = ["price"]
			},
		};

		destination.Datasources.Add(DatasourceRepository.Sales);

		return destination;
	}
}
