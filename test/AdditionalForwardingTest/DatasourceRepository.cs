using InterlinkMapper.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdditionalForwardingTest;

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

}
