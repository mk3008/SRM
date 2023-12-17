using InterlinkMapper.Models;

namespace PostgresTest;

internal static class DestinationRepository
{
	public static InterlinkDestination Customers => new()
	{
		InterlinkDestinationId = 1,
		Table = new()
		{
			TableName = "customer",
			ColumnNames = new() {
				"customer_id",
				"customer_type",
				"customer_name",
			}
		},
		Sequence = new()
		{
			Column = "customer_id",
			Command = "nextval('customer_customer_id_seq'::regclass)"
		},
		ReverseOption = new()
		{
			ExcludedColumns = ["customer_type"],
			ReverseColumns = new()
		},
		Description =
"""
--Table to manage all customers
create table if not exists customer (
	customer_id serial8 not null, 
	customer_type int4 not null, 
	customer_name text not null, 
	created_at timestamp not null default current_timestamp, 
	primary key(customer_id)
);
"""
	};
}
