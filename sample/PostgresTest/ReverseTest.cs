using Carbunql.Dapper;
using Dapper;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;
using System.Data;
using Xunit.Abstractions;

namespace PostgresTest;

public class ReverseTest : IClassFixture<PostgresDB>
{
	public ReverseTest(PostgresDB postgresDB, ITestOutputHelper output)
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
insert into interlink.sale_journal__req_i_sale (sale_id)
select
	sale_id
from
	sale
""");
	}

	private void InsertReverseRequest(IDbConnection cn)
	{
		cn.Execute("""
insert into interlink.sale_journal__req_reverse (sale_journal_id)
select
	sale_journal_id
from
	sale_journal
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
		InsertReverseRequest(cn);

		// execute transfer
		var reverseService = new ReverseForwardingService(Environment);
		reverseService.Execute(cn, source.Destination);

		cnt = cn.ExecuteScalar<int>("select count(*) from sale_journal");
		totalprice = cn.ExecuteScalar<int>("select sum(price) from sale_journal");
		Assert.Equal(20, cnt);
		Assert.Equal(0, totalprice);
	}
}