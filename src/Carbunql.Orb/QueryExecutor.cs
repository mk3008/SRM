using Carbunql.Building;
using Carbunql.Dapper;
using Dapper;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Runtime.CompilerServices;

namespace Carbunql.Orb;

internal class QueryExecutor
{
	public ILogger? Logger { get; init; }

	public int? Timeout { get; set; }

	internal required IDbConnection Connection { private get; set; }

	public T ExecuteScalar<T>(IQueryCommandable query, [CallerMemberName] string memberName = "")
	{
		Logger?.LogInformation(memberName + "\n" + query.ToText() + ";");
		var value = Connection.ExecuteScalar<T>(query, commandTimeout: Timeout);
		if (value == null)
		{
			Logger?.LogInformation($"return : NULL");
		}
		else
		{
			Logger?.LogInformation($"return : {value}");
		}
		return value;
	}

	public T ExecuteScalar<T>(IQueryCommandable query, object? param, [CallerMemberName] string memberName = "")
	{
		var sql = query.ToCommand().CommandText;
		Logger?.LogInformation(memberName + "\n" + sql + ";");
		var value = Connection.ExecuteScalar<T>(sql, param, commandTimeout: Timeout);
		if (value == null)
		{
			Logger?.LogInformation($"return : NULL");
		}
		else
		{
			Logger?.LogInformation($"return : {value}");
		}
		return value;
	}

	public int Execute(IQueryCommandable query, [CallerMemberName] string memberName = "")
	{
		Logger?.LogInformation(memberName + "\n" + query.ToText() + ";");
		var count = Connection.Execute(query, commandTimeout: Timeout);
		Logger?.LogInformation($"results : {count} row(s)");
		return count;
	}

	public int CreateTable(SelectQuery query, string tableName, bool isTemporary = true, [CallerMemberName] string memberName = "")
	{
		var createQuery = query.ToCreateTableQuery(tableName, isTemporary);
		Logger?.LogInformation(memberName + "\n" + createQuery.ToText() + ";");
		Connection.Execute(createQuery, commandTimeout: Timeout);

		var countQuery = new SelectQuery();
		countQuery.From(tableName);
		countQuery.Select("count(*)");

		return ExecuteScalar<int>(countQuery, memberName);
	}
}
