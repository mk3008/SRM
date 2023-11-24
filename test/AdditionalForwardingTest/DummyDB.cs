using InterlinkMapper.Models;
using RedOrb;

namespace AdditionalForwardingTest;

public class DummyDB : IDbConnetionSetting
{
	public LoggingDbConnection ConnectionOpenAsNew()
	{
		throw new NotImplementedException();
	}
}