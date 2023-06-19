using Npgsql;
using System.Data;

namespace Carbunql.Orb.Test;

internal class PostgresDB
{
	public IDbConnection ConnectionOpenAsNew()
	{
		var cnstring = "Server=localhost;Port=5430;Database=postgres;User Id=root;Password=root;";

		var cn = new NpgsqlConnection(cnstring);
		cn.Open();
		return cn;
	}
}
