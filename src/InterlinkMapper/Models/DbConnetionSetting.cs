using RedOrb;

namespace InterlinkMapper.Models;

public interface DbConnetionSetting
{
	LoggingDbConnection ConnectionOpenAsNew();
}
