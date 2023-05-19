using System.Data;

namespace InterlinkMapper.System;

public interface IDbConnetionConfig
{
	IDbConnection ConnectionOpenAsNew();
}
