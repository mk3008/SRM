using Carbunql.Analysis.Parser;
using Carbunql.Building;
using Cysharp.Text;

namespace Carbunql.Orb;

public interface IDbTableDefinition : IDbTable
{
	IEnumerable<DbColumnDefinition> ColumnDefinitions { get; }

	List<DbIndexDefinition> Indexes { get; }
}

public static class IDbTableDefinitionExtention
{
	public static string ToCreateTableCommandText(this IDbTableDefinition source)
	{
		var table = ValueParser.Parse(source.GetTableFullName()).ToText();

		var sb = ZString.CreateStringBuilder();
		foreach (var column in source.ColumnDefinitions)
		{
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("    " + column.ToCommandText());
		}

		var pkeys = source.ColumnDefinitions.Where(x => x.IsPrimaryKey).ToList();
		if (pkeys.Any())
		{
			var columnText = string.Join(", ", pkeys.Select(x => ValueParser.Parse(x.ColumnName).ToText()));
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("primary key(" + string.Join(", ", columnText) + ")");
		}

		var ukeys = source.ColumnDefinitions.Where(x => x.IsUniqueKey).ToList();
		if (ukeys.Any())
		{
			var columnText = string.Join(", ", ukeys.Select(x => ValueParser.Parse(x.ColumnName).ToText()));
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("unique(" + string.Join(", ", columnText) + ")");
		}

		var sql = @$"create table if not exists {table} (
{sb}
)";
		return sql;
	}

	public static IEnumerable<string> ToCreateIndexCommandTexts(this IDbTableDefinition source)
	{
		var id = 0;
		foreach (var index in source.Indexes)
		{
			id++;
			yield return index.ToCommandText(source, id);
		}
	}

	public static string GetColumnName(this IDbTableDefinition source, string identifer)
	{
		return source.ColumnDefinitions.Where(x => x.Identifer == identifer).Select(x => x.ColumnName).First();
	}

	public static DbColumnDefinition? GetSequenceOrDefault(this IDbTableDefinition source)
	{
		return source.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();
	}

	public static List<DbColumnDefinition> GetPrimaryKeys(this IDbTableDefinition source)
	{
		var lst = source.ColumnDefinitions.Where(x => x.IsPrimaryKey && !string.IsNullOrEmpty(x.Identifer)).ToList();
		if (!lst.Any()) throw new NotSupportedException("Primary key column not found.");
		return lst;
	}

	public static SelectQuery ToSelectQuery(this IDbTableDefinition source)
	{
		var table = ValueParser.Parse(source.GetTableFullName()).ToText();

		var sq = new SelectQuery();
		var (_, t) = sq.From(table).As("t");

		foreach (var column in source.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(column.Identifer)) continue;
			sq.Select(t, column.ColumnName).As(column.Identifer);
		}

		return sq;
	}

	//public static SelectQuery AddInnerJoin(this IDbTableDefinition source, IDbTableDefinition joinFromDefinition, SelectQuery sq)
	//{
	//	var joinToTable = source.GetTableFullName();
	//	if (string.IsNullOrEmpty(joinToTable)) throw new InvalidOperationException();

	//	var pkeys = source.GetPrimaryKeys().Select(x => x.ColumnName);
	//	if (!pkeys.Any()) throw new InvalidOperationException();

	//	var joinFromTable = joinFromDefinition.GetTableFullName();

	//	var f = sq.FromClause;
	//	if (f == null) throw new InvalidOperationException();

	//	var joinFrom = f.GetSelectableTables().Reverse().Where(x => x.Table.GetTableFullName() == joinFromTable).FirstOrDefault();
	//	if (joinFrom == null) throw new InvalidOperationException();

	//	var index = f.GetSelectableTables().Count();
	//	var t = f.InnerJoin(joinToTable).As("t" + index).On(joinFrom, pkeys);

	//	foreach (var column in source.ColumnDefinitions)
	//	{
	//		if (string.IsNullOrEmpty(column.Identifer)) continue;
	//		sq.Select(t, column.ColumnName).As(column.Identifer);
	//	}

	//	return sq;
	//}
}