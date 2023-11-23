using Cysharp.Text;

namespace InterlinkMapper.Models;

public class DbIndexDefinition
{
	public int IndexNumber { get; set; } = 1;

	public bool IsUnique { get; set; } = false;

	public List<string> Columns { get; set; } = new();
}

public static class DbIndexDefinitionExtension
{
	public static string ToCreateCommandText(this DbIndexDefinition source, IDbTable table)
	{
		var name = $"i{source.IndexNumber}_{table.TableName}";
		var indextype = source.IsUnique ? "unique index" : "index";
		var sb = ZString.CreateStringBuilder();
		foreach (var column in source.Columns)
		{
			if (sb.Length > 0)
			{
				sb.Append(", ");
			}
			sb.Append(column);
		}

		var sql = @$"create {indextype} if not exists {name} on {table.GetTableFullName()} ({sb})";
		return sql;
	}
}
