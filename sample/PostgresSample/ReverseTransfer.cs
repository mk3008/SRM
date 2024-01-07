using Dapper;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using RedOrb;

namespace PostgresSample;

internal static class ReverseTransfer
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
insert into interlink.sale_journal__req_reverse (sale_journal_id)
select
	sale_journal_id
from
	sale_journal
""");
	}

	public static void Execute()
	{
		using var cn = PostgresDB.ConnectionOpenAsNew(new ConsoleLogger());
		using var trn = cn.BeginTransaction();

		// load datasource
		var datasource = cn.Load(new InterlinkDestination()
		{
			DbSequence = null!,
			DbTable = null!,
			ReverseOption = null!,
			TableFullName = "sale_journal"
		});

		// execute transfer
		var service = new ReverseForwardingService(ApplicationInitializer.GetEnvironment());

		/*
		 * NOTE:
		 * The sample uses all requests.
		 * If you want to filter requests, use an injector.
		 */
		service.Execute(cn, datasource);

		trn.Commit();
	}
}
