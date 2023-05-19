using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.System;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Runtime.CompilerServices;

namespace InterlinkMapper.Services;

public interface IQueryExecuteService
{
	IDbConnection Connection { get; }

	int ProcessId { get; }

	ILogger? Logger { get; }

	SystemEnvironment Environment { get; }
}

public static class QueryExecuteServiceExtention
{

	public static int CreateTable(this IQueryExecuteService service, SelectQuery query, string tableName, bool isTemporary = true, int commandTimeout = 180, [CallerMemberName] string memberName = "")
	{
		var q = query.ToCreateTableQuery(tableName, isTemporary);

		service.Logger?.LogInformation(q.ToText() + ";");
		service.Connection.Execute(query, commandTimeout: commandTimeout);

		var sq = new SelectQuery();
		sq.From(tableName);
		sq.Select("count(*)");
		var cnt = service.Connection.ExecuteScalar<int>(sq);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.CreateTable, tableName, cnt, commandTimeout, memberName);

		return cnt;
	}

	public static int Insert(this IQueryExecuteService service, SelectQuery query, IDbTable destination, int commandTimeout = 180, [CallerMemberName] string memberName = "")
	{
		var q = query.ToInsertQuery(destination.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");

		var cnt = service.Connection.Execute(query, commandTimeout: commandTimeout);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.Insert, destination.GetTableFullName(), cnt, commandTimeout, memberName);

		return cnt;
	}

	public static int Delete(this IQueryExecuteService service, SelectQuery query, IDbTable destination, int commandTimeout = 180, [CallerMemberName] string memberName = "")
	{
		var q = query.ToDeleteQuery(destination.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");

		var cnt = service.Connection.Execute(query, commandTimeout: commandTimeout);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.Delete, destination.GetTableFullName(), cnt, commandTimeout, memberName);

		return cnt;
	}

	private static void WriteQueryRestultLog(this IQueryExecuteService service, QueryAction action, string table, int count, int commandTimeout, string memberName)
	{
		var dbQueryConfig = service.Environment.DbQueryConfig;
		var dbTableConfig = service.Environment.DbTableConfig;

		var sq = new SelectQuery();
		sq.Select(dbQueryConfig.PlaceHolderIdentifer, dbTableConfig.ProcessIdColumn, service.ProcessId);
		sq.Select(dbQueryConfig.PlaceHolderIdentifer, dbTableConfig.MemberNameColumn, memberName);
		sq.Select(dbQueryConfig.PlaceHolderIdentifer, dbTableConfig.ActionNameColumn, action.ToString());
		sq.Select(dbQueryConfig.PlaceHolderIdentifer, dbTableConfig.TableNameColumn, table);
		sq.Select(dbQueryConfig.PlaceHolderIdentifer, dbTableConfig.ResultCountColumn, count);

		var q = sq.ToInsertQuery(dbTableConfig.ProcessResultTable.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");
		service.Connection.Execute(q, commandTimeout: commandTimeout);
	}
}
