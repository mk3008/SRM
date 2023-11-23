namespace InterlinkMapper.Models;

public interface IDbTable
{
	string SchemaName { get; set; }

	string TableName { get; set; }

	IEnumerable<string> Columns { get; }
}

public static class IDbTableExtention
{
	public static string GetTableFullName(this IDbTable source)
	{
		return string.IsNullOrEmpty(source.SchemaName) ? source.TableName : source.SchemaName + "." + source.TableName;
	}
}
