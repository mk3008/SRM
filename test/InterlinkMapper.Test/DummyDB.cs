using InterlinkMapper.Models;
using RedOrb;

namespace InterlinkMapper.Test;

public class DummyDB : IDbConnetionSetting
{
	public LoggingDbConnection ConnectionOpenAsNew()
	{
		throw new NotImplementedException();
	}
}