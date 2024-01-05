using InterlinkMapper.Models;

namespace PostgresTest;

internal static class SystemRepository
{
	internal static InterlinkTransaction GetDummyTransactionRow(InterlinkDestination destination)
	{
		return new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = "test",
			Argument = "argument"
		};
	}

	internal static InterlinkProcess GetDummyProcessRow(InterlinkDatasource source)
	{
		return new InterlinkProcess()
		{
			InterlinkDatasource = source,
			ActionName = "test",
			InterlinkTransaction = GetDummyTransactionRow(source.Destination),
			InsertCount = 100
		};
	}
}
