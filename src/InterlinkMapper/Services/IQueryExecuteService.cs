using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using RedOrb;
using System.Data;
using System.Runtime.CompilerServices;

namespace InterlinkMapper.Services;

//public interface IQueryExecuteService
//{
//	LoggingDbConnection Connection { get; }

//	//int ProcessId { get; }

//	int CommandTimeout { get; set; }

//	SystemEnvironment Environment { get; }
//}

//public static class IQueryExecuteServiceExtention
//{
//	public static SelectQuery ToSelectQuery(this string bridgeTable, List<string> columns)
//	{
//		var sq = new SelectQuery();
//		var (_, b) = sq.From(bridgeTable).As("b");
//		columns.ForEach(x => sq.Select(b, x));
//		return sq;
//	}

//	public static int CreateTable(this IQueryExecuteService service, SelectQuery query, string tableName, bool isTemporary = true, [CallerMemberName] string memberName = "")
//	{
//		var createQuery = query.ToCreateTableQuery(tableName, isTemporary);

//		service.Connection.LogInformation(createQuery.ToText() + ";");
//		service.Connection.Execute(createQuery, commandTimeout: service.CommandTimeout);

//		var countQuery = new SelectQuery();
//		countQuery.From(tableName);
//		countQuery.Select("count(*)");
//		service.Connection.LogInformation(countQuery.ToText() + ";");

//		var cnt = service.Connection.ExecuteScalar<int>(countQuery, commandTimeout: service.CommandTimeout);
//		service.Connection.LogInformation("results : {cnt} row(s)", cnt);

//		service.WriteQueryResultLog(QueryAction.CreateTable, tableName, cnt, memberName);

//		return cnt;
//	}

//	public static int Insert(this IQueryExecuteService service, SelectQuery query, IDbTable destination, [CallerMemberName] string memberName = "")
//	{
//		var tmp = new SelectQuery();
//		var (_, d) = tmp.From(query).As("d");
//		tmp.Select(d);
//		tmp.SelectClause!.FilterInColumns(destination.Columns);
//		var q = tmp.ToInsertQuery(destination.GetTableFullName());

//		service.Connection.LogInformation(q.ToText() + ";");

//		var cnt = service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
//		service.Connection.LogInformation("results : {cnt} row(s)", cnt);

//		service.WriteQueryResultLog(QueryAction.Insert, destination.GetTableFullName(), cnt, memberName);

//		return cnt;
//	}

//	public static int Insert(this IQueryExecuteService service, SelectQuery query, string destination, [CallerMemberName] string memberName = "")
//	{
//		var tmp = new SelectQuery();
//		var (_, d) = tmp.From(query).As("d");
//		tmp.Select(d);
//		var q = tmp.ToInsertQuery(destination);

//		service.Connection.LogInformation(q.ToText() + ";");

//		var cnt = service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
//		service.Connection.LogInformation("results : {cnt} row(s)", cnt);

//		service.WriteQueryResultLog(QueryAction.Insert, destination, cnt, memberName);

//		return cnt;
//	}

//	public static int Delete(this IQueryExecuteService service, SelectQuery query, IDbTable destination, [CallerMemberName] string memberName = "")
//	{
//		var q = query.ToDeleteQuery(destination.GetTableFullName());

//		service.Connection.LogInformation(q.ToText() + ";");

//		var cnt = service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
//		service.Connection.LogInformation("results : {cnt} row(s)", cnt);

//		service.WriteQueryResultLog(QueryAction.Delete, destination.GetTableFullName(), cnt, memberName);

//		return cnt;
//	}

//	public static int Delete(this IQueryExecuteService service, SelectQuery query, string destination, [CallerMemberName] string memberName = "")
//	{
//		var q = query.ToDeleteQuery(destination);

//		service.Connection.LogInformation(q.ToText() + ";");

//		var cnt = service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
//		service.Connection.LogInformation("results : {cnt} row(s)", cnt);

//		service.WriteQueryResultLog(QueryAction.Delete, destination, cnt, memberName);

//		return cnt;
//	}

//private static void WriteQueryResultLog(this IQueryExecuteService service, QueryAction action, string table, int count, string memberName)
//{
//	var qc = service.Environment.DbEnvironment;
//	var tc = service.Environment.DbTableConfig;

//	var sq = new SelectQuery();
//	sq.Select(qc.PlaceHolderIdentifer, tc.ProcessIdColumn, service.ProcessId);
//	sq.Select(qc.PlaceHolderIdentifer, tc.MemberNameColumn, memberName);
//	sq.Select(qc.PlaceHolderIdentifer, tc.ActionNameColumn, action.ToString());
//	sq.Select(qc.PlaceHolderIdentifer, tc.TableNameColumn, table);
//	sq.Select(qc.PlaceHolderIdentifer, tc.ResultCountColumn, count);

//	var q = sq.ToInsertQuery(tc.ProcessResultTable.GetTableFullName());

//	service.Connection.LogInformation(q.ToText() + ";");
//	service.Connection.Execute(q, commandTimeout: service.CommandTimeout);
//}
//}

//public enum QueryAction
//{
//	CreateTable,
//	Insert,
//	Update,
//	Delete,
//}
