using Carbunql.Analysis.Parser;
using Cysharp.Text;

namespace InterlinkMapper.Data;

public interface IDbTable
{
	string SchemaName { get; set; }

	string TableName { get; set; }

	IEnumerable<String> Columns { get; }
}

public interface IDbTableDefinition : IDbTable
{
	IEnumerable<ColumnDefinition> ColumnDefinitions { get; }
}

public static class ITableExtention
{
	public static string GetTableFullName(this IDbTable source)
	{
		return string.IsNullOrEmpty(source.SchemaName) ? source.TableName : source.SchemaName + "." + source.TableName;
	}
}

public static class ITableDefinitionExtention
{
	public static string ToCreateCommandText(this IDbTableDefinition source)
	{
		var name = ValueParser.Parse(source.GetTableFullName()).ToText();

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

		var sql = @$"create table if not exists {name} (
{sb}
)";
		return sql;
	}
}

