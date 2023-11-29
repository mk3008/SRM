using InterlinkMapper.Models;

namespace InterlinkMapper.Test;

internal static class DatasourceRepository
{

	public static DbDatasource sales =>
		new DbDatasource()
		{
			DatasourceId = 1,
			DatasourceName = "sales",
			Destination = DestinationRepository.sale_journals,
			KeyColumns = new() {
					new () {
						ColumnName = "sale_id",
						TypeName = "int8"
					}
			},
			KeyName = "sales",
			Query = @"
select
	s.sale_date as journal_closing_date,
	s.sale_date,
	s.shop_id,
	s.price,
	--key
	s.sale_id	
from
	sales as s
	",
		};

	public static DbDatasource cte_sales =>
		new DbDatasource()
		{
			DatasourceId = 1,
			DatasourceName = "sales",
			Destination = DestinationRepository.sale_journals,
			KeyColumns = new() {
					new () {
						ColumnName = "sale_id",
						TypeName = "int8"
					}
			},
			KeyName = "sales",
			Query = @"
with
__raw as (
	select
		s.sale_date as journal_closing_date,
		s.sale_date,
		s.shop_id,
		s.price,
		s.sale_id, 
		s.sale_detail_id
	from
		sale_detail as s
)
select
	d.journal_closing_date,
	d.sale_date,
	d.shop_id,
	sum(d.price) as price,
	--key
	d.sale_id	
from
	__raw as d
group by
	d.journal_closing_date,
	d.sale_date,
	d.shop_id,
	d.sale_id	
",
		};

}
