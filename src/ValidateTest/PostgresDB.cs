using InterlinkMapper.System;
using Npgsql;
using System.Data;

namespace ValidateTest;

internal class PostgresDB : IDbConnetionConfig
{
	public IDbConnection ConnectionOpenAsNew()
	{
		var cnstring = "Server=localhost;Port=5430;Database=postgres;User Id=root;Password=root;";

		var cn = new NpgsqlConnection(cnstring);
		cn.Open();
		return cn;
	}
}
