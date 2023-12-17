using InterlinkMapper.Models;

namespace PostgresTest;

internal static class DatasourceRepository
{
	public static InterlinkDatasource NetMembers =>
		new()
		{
			InterlinkDatasourceId = 1,
			DatasourceName = "NetMembers",
			Destination = DestinationRepository.Customers,
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
			Description =
"""
--online shop customers
create table if not exists net_member (
    net_member_id serial8 not null, 
    user_name text not null, 
    created_at timestamp not null default current_timestamp, 
    primary key(net_member_id)
)
;
insert into net_member (user_name)
select
    'user ' || generate_series(1, 10)
;
"""
		};

	public static InterlinkDatasource CorporateCustomers =>
		new()
		{
			InterlinkDatasourceId = 2,
			DatasourceName = "CorporateCustomers",
			Destination = DestinationRepository.Customers,
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
			Description =
"""
--Corporate customers
create table if not exists corporate_customer (
    corporate_customer_id serial8 not null, 
    company_name text not null, 
    created_at timestamp not null default current_timestamp, 
    primary key(corporate_customer_id)
)
;
insert into corporate_customer (company_name)
select
    'company ' || generate_series(1, 10)
;
"""
		};
}
