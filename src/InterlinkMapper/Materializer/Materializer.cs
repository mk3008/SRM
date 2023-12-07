using System.Data;

namespace InterlinkMapper.Materializer;

internal interface IMaterializer
{
	int CommandTimeout { get; }
}

internal static class IMaterializerExtension
{
	internal static Material CreateMaterial(this IMaterializer materializer, IDbConnection connection, CreateTableQuery query)
	{
		var tableName = query.TableFullName;

		connection.Execute(query, commandTimeout: materializer.CommandTimeout);
		var rows = connection.ExecuteScalar<int>(query.ToCountQuery());

		return new Material
		{
			Count = rows,
			MaterialName = tableName,
			SelectQuery = query.ToSelectQuery(),
		};
	}
}