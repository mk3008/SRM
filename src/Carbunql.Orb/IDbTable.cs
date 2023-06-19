namespace Carbunql.Orb;

public interface IDbTable
{
	string SchemaName { get; }

	string TableName { get; }

	IEnumerable<string> Columns { get; }
}


public static class IDbTableExtention
{
	public static string GetTableFullName(this IDbTable source)
	{
		return string.IsNullOrEmpty(source.SchemaName) ? source.TableName : source.SchemaName + "." + source.TableName;
	}
}