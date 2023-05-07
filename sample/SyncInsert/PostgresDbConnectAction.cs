using InterlinkMapper.Actions;
using Npgsql;
using System.Data;

namespace SyncInsert;

internal class PostgresDbConnectAction : IDbConnectAction
{
	public IDbConnection Execute()
	{
		var cnstring = "Server=localhost;Port=5430;Database=postgres;User Id=root;Password=root;";

		var cn = new NpgsqlConnection(cnstring);
		cn.Open();
		return cn;
	}
}
