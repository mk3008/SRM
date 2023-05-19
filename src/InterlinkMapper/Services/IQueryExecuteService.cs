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

	int CommandTimeout { get; set; }

	SystemEnvironment Environment { get; }
}

public static class IQueryExecuteServiceExtention
{

	public static int CreateTable(this IQueryExecuteService service, SelectQuery query, string tableName, bool isTemporary = true, [CallerMemberName] string memberName = "")
	{
		var createQuery = query.ToCreateTableQuery(tableName, isTemporary);

		service.Logger?.LogInformation(createQuery.ToText() + ";");
		service.Connection.Execute(createQuery, commandTimeout: service.CommandTimeout);

		var countQuery = new SelectQuery();
		countQuery.From(tableName);
		countQuery.Select("count(*)");
		service.Logger?.LogInformation(countQuery.ToText() + ";");

		var cnt = service.Connection.ExecuteScalar<int>(countQuery, commandTimeout: service.CommandTimeout);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.CreateTable, tableName, cnt, memberName);

		return cnt;
	}

	public static int Insert(this IQueryExecuteService service, SelectQuery query, IDbTable destination, [CallerMemberName] string memberName = "")
	{
		var q = query.ToInsertQuery(destination.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");

		var cnt = service.Connection.Execute(query, commandTimeout: service.CommandTimeout);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.Insert, destination.GetTableFullName(), cnt, memberName);

		return cnt;
	}

	public static int Delete(this IQueryExecuteService service, SelectQuery query, IDbTable destination, [CallerMemberName] string memberName = "")
	{
		var q = query.ToDeleteQuery(destination.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");

		var cnt = service.Connection.Execute(query, commandTimeout: service.CommandTimeout);
		service.Logger?.LogInformation("results : {cnt} row(s)", cnt);

		service.WriteQueryRestultLog(QueryAction.Delete, destination.GetTableFullName(), cnt, memberName);

		return cnt;
	}

	private static void WriteQueryRestultLog(this IQueryExecuteService service, QueryAction action, string table, int count, string memberName)
	{
		var qc = service.Environment.DbQueryConfig;
		var tc = service.Environment.DbTableConfig;

		var sq = new SelectQuery();
		sq.Select(qc.PlaceHolderIdentifer, tc.ProcessIdColumn, service.ProcessId);
		sq.Select(qc.PlaceHolderIdentifer, tc.MemberNameColumn, memberName);
		sq.Select(qc.PlaceHolderIdentifer, tc.ActionNameColumn, action.ToString());
		sq.Select(qc.PlaceHolderIdentifer, tc.TableNameColumn, table);
		sq.Select(qc.PlaceHolderIdentifer, tc.ResultCountColumn, count);

		var q = sq.ToInsertQuery(tc.ProcessResultTable.GetTableFullName());

		service.Logger?.LogInformation(q.ToText() + ";");
		service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
	}
}


public enum QueryAction
{
	CreateTable,
	Insert,
	Update,
	Delete,
}
