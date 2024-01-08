using InterlinkMapper.Models;
using System.Data;

namespace InterlinkMapper.Materializer;

internal interface IRequestMaterializer
{
	int CommandTimeout { get; }
}

internal static class RequestMaterializerExtension
{
	internal static Material CreateMaterial(this IRequestMaterializer materializer, IDbConnection connection, InterlinkTransaction transaction, CreateTableQuery query)
	{
		var tableName = query.TableFullName;

		connection.Execute(query, commandTimeout: materializer.CommandTimeout);
		var rows = connection.ExecuteScalar<int>(query.ToCountQuery());

		return new Material
		{
			InterlinkTransaction = transaction,
			MaterialName = tableName,
			SelectQuery = query.ToSelectQuery(),
			Count = rows,
		};
	}
}