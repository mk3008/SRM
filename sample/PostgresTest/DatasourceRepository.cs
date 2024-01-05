using InterlinkMapper.Models;

namespace PostgresTest;

internal static class DatasourceRepository
{
	public static IEnumerable<InterlinkDatasource> GetAll()
	{
		yield return NetMembers;
		yield return CorporateCustomers;
		yield return Sales;
	}

	public static InterlinkDatasource NetMembers =>
		new()
		{
			InterlinkDatasourceId = 0,
			DatasourceName = "NetMembers",
			Destination = null!,
			KeyColumns = new() {
				new KeyColumn {
					ColumnName = "net_member_id",
					TypeName = "int8"
				}
			},
			KeyName = "net_member",
			Query =
"""
select
	2 as customer_type
	, nm.user_name as customer_name
	--key
	, nm.net_member_id	
from
	net_member as nm
""",
			Description = ""
		};

	public static InterlinkDatasource CorporateCustomers =>
		new()
		{
			InterlinkDatasourceId = 2,
			DatasourceName = "CorporateCustomers",
			Destination = null!,
			KeyColumns = new() {
				new KeyColumn {
					ColumnName = "corporate_customer_id",
					TypeName = "int8"
				}
			},
			KeyName = "corporate_customer",
			Query =
"""
select
	1 as customer_type
	, cc.company_name as customer_name
	--key
	, cc.corporate_customer_id		
from
	corporate_customer as cc
""",
			Description = ""
		};

	public static InterlinkDatasource Sales =>
		new()
		{
			InterlinkDatasourceId = 3,
			DatasourceName = "Sales",
			Destination = null!,
			KeyColumns = new() {
				new KeyColumn {
					ColumnName = "sale_id",
					TypeName = "int8"
				}
			},
			KeyName = "sale",
			Query =
"""
select
	s.sale_date
	, s.price
	--key
	, s.sale_id		
from
	sale as s
""",
			Description = ""
		};
}
