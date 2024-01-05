using InterlinkMapper.Models;

namespace InterlinkMapper.Test;

internal static class SystemRepository
{
	internal static InterlinkTransaction GetDummyTransaction(InterlinkDestination destination)
	{
		return new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = "test",
			Argument = "argument"
		};
	}

	internal static InterlinkProcess GetDummyProcess(InterlinkDatasource source)
	{
		return new InterlinkProcess()
		{
			InterlinkDatasource = source,
			ActionName = "test",
			InterlinkTransaction = GetDummyTransaction(source.Destination),
			InsertCount = 100
		};
	}
}
