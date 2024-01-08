using Dapper;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;

namespace PostgresSample;

internal static class ValidationTransfer
{
	public static void CreateRequest()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(new ConsoleLogger());

		/*
		NOTE:
		The sample creates the request manually.
		Actually, make the request with an update trigger or something like that.
		*/
		cn.Execute("""
update sale
set
	price = price + 30
where
	sale_id = 1
;
insert into interlink.sale_journal__req_v_sale (sale_id)
select
	s.sale_id
from
	sale as s
where
	s.sale_id not in (select x.sale_id from interlink.sale_journal__req_v_sale as x)
	and s.sale_id = 1
;
""");
	}

	public static void Execute()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(new ConsoleLogger());
		using var trn = cn.BeginTransaction();

		// load datasource
		var datasource = cn.Load(new InterlinkDatasource()
		{
			DatasourceName = null!,
			Destination = null!,
			KeyColumns = null!,
			KeyName = "sale"!,
			Query = null!,
		});

		// execute transfer
		var service = new ValidationForwardingService(ApplicationInitializer.GetEnvironment());

		/*
		 * NOTE:
		 * The sample uses all requests.
		 * If you want to filter requests, use an injector.
		 */
		service.Execute(cn, datasource);

		trn.Commit();
	}
}
