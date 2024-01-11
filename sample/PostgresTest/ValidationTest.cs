using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;
using System.Data;
using Xunit.Abstractions;

namespace PostgresTest;

public class ValidationTest : IClassFixture<PostgresDB>
{
	public ValidationTest(PostgresDB postgresDB, ITestOutputHelper output)
	{
		PostgresDB = postgresDB;
		Logger = new UnitTestLogger() { Output = output };
		Environment = UnitTestInitializer.GetEnvironment();
	}

	private readonly PostgresDB PostgresDB;

	private readonly UnitTestLogger Logger;

	private readonly SystemEnvironment Environment;

	private void InsertAdditionalRequest(IDbConnection cn)
	{
		cn.Execute("""
--request fro addlition
insert into interlink.sale_journal__req_i_sale (sale_id)
select
	sale_id
from
	sale
""");
	}

	private void InsertValidationRequest(IDbConnection cn)
	{
		cn.Execute("""
--request for validation
insert into interlink.sale_journal__req_v_sale (sale_id)
select
	sale_id
from
	sale
""");
	}

	private void UpdateDatasourceValue(IDbConnection cn)
	{
		cn.Execute("""
--request for validation
update sale
set
	price = 200
where
	sale_date = '2023-01-01'
	and price = 100
""");
	}

	[Fact]
	public void Default()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(Logger);
		using var trn = cn.BeginTransaction();

		// prepare test data
		UnitTestInitializer.CreateTableAndData(cn);
		InsertAdditionalRequest(cn);

		// execute transfer
		var source = cn.Load(DatasourceRepository.Sales);
		var additionalService = new AdditionalForwardingService(Environment);
		additionalService.Execute(cn, source);

		var relationcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__relation");
		var keymapcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__key_m_sale");
		var keyrelationcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__key_r_sale");
		var cnt = cn.ExecuteScalar<int>("select count(*) from sale_journal");
		var totalprice = cn.ExecuteScalar<int>("select sum(price) from sale_journal");
		Assert.Equal(10, relationcount);
		Assert.Equal(10, keymapcount);
		Assert.Equal(10, keyrelationcount);
		Assert.Equal(10, cnt);
		Assert.NotEqual(0, totalprice);

		InsertValidationRequest(cn);

		var reverseService = new ValidationForwardingService(Environment);
		reverseService.Execute(cn, source);

		// not changed
		cnt = cn.ExecuteScalar<int>("select count(*) from sale_journal");
		totalprice = cn.ExecuteScalar<int>("select sum(price) from sale_journal");
		Assert.Equal(10, cnt);
		Assert.NotEqual(0, totalprice);
	}

	[Fact]
	public void ValidateTest_Update()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(Logger);
		using var trn = cn.BeginTransaction();

		// prepare test data
		UnitTestInitializer.CreateTableAndData(cn);
		InsertAdditionalRequest(cn);

		// execute transfer
		var source = cn.Load(DatasourceRepository.Sales);
		var additionalService = new AdditionalForwardingService(Environment);
		additionalService.Execute(cn, source);

		var relationcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__relation");
		var keymapcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__key_m_sale");
		var keyrelationcount = cn.ExecuteScalar<int>("select count(*) from interlink.sale_journal__key_r_sale");
		var cnt = cn.ExecuteScalar<int>("select count(*) from sale_journal");
		var totalprice = cn.ExecuteScalar<int>("select sum(price) from sale_journal");
		Assert.Equal(10, relationcount);
		Assert.Equal(10, keymapcount);
		Assert.Equal(10, keyrelationcount);
		Assert.Equal(10, cnt);
		Assert.NotEqual(0, totalprice);

		// value change
		UpdateDatasourceValue(cn);
		InsertValidationRequest(cn);

		var reverseService = new ValidationForwardingService(Environment);
		reverseService.Execute(cn, source);

		// not changed
		cnt = cn.ExecuteScalar<int>("select count(*) from sale_journal");
		totalprice = cn.ExecuteScalar<int>("select sum(price) from sale_journal");
		Assert.Equal(12, cnt);
		Assert.NotEqual(0, totalprice);
	}
}
