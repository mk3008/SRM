using RedOrb;

namespace InterlinkMapper.Models;

public interface IDbConnetionSetting
{
	LoggingDbConnection ConnectionOpenAsNew();
}
