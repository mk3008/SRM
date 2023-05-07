using System.Data;

namespace InterlinkMapper.Actions;

public interface IDbConnectAction
{
	IDbConnection Execute();
}
