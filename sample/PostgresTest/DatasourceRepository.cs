using InterlinkMapper.Models;

namespace PostgresTest;

internal static class DatasourceRepository
{
	public static InterlinkDatasource sales =>
		new()
		{
			InterlinkDatasourceId = 1,
			DatasourceName = "sales",
			Destination = DestinationRepository.sale_journals,
			KeyColumns = new() {
				new KeyColumn {
					ColumnName = "sale_id",
					TypeName = "int8"
				}
			},
			KeyName = "sales",
			Query = """
select
	s.sale_date as journal_closing_date,
	s.sale_date,
	s.shop_id,
	s.price,
	--key
	s.sale_id	
from
	sales as s
""",
		};
}
