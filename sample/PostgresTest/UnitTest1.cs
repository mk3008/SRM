using Carbunql.Dapper;
using Dapper;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;
using System.Data;
using Xunit.Abstractions;

namespace PostgresTest;

public class UnitTest1 : IClassFixture<PostgresDB>
{
	public UnitTest1(PostgresDB postgresDB, ITestOutputHelper output)
	{
		PostgresDB = postgresDB;
		Logger = new UnitTestLogger() { Output = output };

		using var cn = PostgresDB.ConnectionOpenAsNew(Logger);

		Environment = new SystemEnvironment();
		Environment.DbTableConfig.ControlTableSchemaName = "interlink";
	}

	private readonly PostgresDB PostgresDB;

	private readonly UnitTestLogger Logger;

	private readonly SystemEnvironment Environment;

	private void CreateTableAndData(IDbConnection cn)
	{
		var netMembers = DatasourceRepository.NetMembers;
		var corporateCustomers = DatasourceRepository.CorporateCustomers;
		var customers = DestinationRepository.Customers;

		cn.Execute($"create schema if not exists {Environment.DbTableConfig.ControlTableSchemaName};");

		cn.CreateTableOrDefault(Environment.GetInterlinkTansactionTable().Definition);
		cn.CreateTableOrDefault(Environment.GetInterlinkProcessTable().Definition);

		cn.CreateTableOrDefault(Environment.GetInterlinkRelationTable(customers).Definition);
		cn.CreateTableOrDefault(Environment.GetReverseRequestTable(customers).Definition);

		cn.CreateTableOrDefault(Environment.GetKeyMapTable(netMembers).Definition);
		cn.CreateTableOrDefault(Environment.GetKeyRelationTable(netMembers).Definition);
		cn.CreateTableOrDefault(Environment.GetInsertRequestTable(netMembers).Definition);
		cn.CreateTableOrDefault(Environment.GetValidationRequestTable(netMembers).Definition);

		cn.CreateTableOrDefault(Environment.GetKeyMapTable(corporateCustomers).Definition);
		cn.CreateTableOrDefault(Environment.GetKeyRelationTable(corporateCustomers).Definition);
		cn.CreateTableOrDefault(Environment.GetInsertRequestTable(corporateCustomers).Definition);
		cn.CreateTableOrDefault(Environment.GetValidationRequestTable(corporateCustomers).Definition);

		cn.Execute("""
--Table to manage all customers
create table if not exists customer (
	customer_id serial8 not null, 
	customer_type int4 not null, 
	customer_name text not null, 
	created_at timestamp not null default current_timestamp, 
	primary key(customer_id)
)
;
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
""");
	}

	private void InsertAdditionalRequest(IDbConnection cn)
	{
		cn.Execute("""
insert into interlink.customer__req_i_net_member (net_member_id)
select
	net_member_id
from
	net_member
""");
	}

	[Fact]
	public void AdditionalTest()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(Logger);
		using var trn = cn.BeginTransaction();

		// prepare test data
		CreateTableAndData(cn);
		InsertAdditionalRequest(cn);

		var beforecnt = cn.ExecuteScalar<int>("select count(*) from customer");
		var expectcnt = cn.ExecuteScalar<int>("select count(*) from net_member");
		var beforerequest = cn.ExecuteScalar<int>("select count(*) from interlink.customer__req_i_net_member");

		// execute transfer
		var service = new AdditionalForwardingService(Environment);
		service.Execute(cn, DatasourceRepository.NetMembers, injector: null);

		// validate
		var actualcnt = cn.ExecuteScalar<int>("select count(*) from customer");
		var afterrequest = cn.ExecuteScalar<int>("select count(*) from interlink.customer__req_i_net_member");

		// The data at the forwarding destination has increased by the amount requested.
		Assert.Equal(0, beforecnt);
		Assert.NotEqual(0, expectcnt);
		Assert.Equal(expectcnt, actualcnt);

		// The request will be deleted once the transfer process is complete.
		Assert.NotEqual(0, beforerequest);
		Assert.Equal(0, afterrequest);
	}
}