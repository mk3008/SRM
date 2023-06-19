using Carbunql.Analysis.Parser;
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
}