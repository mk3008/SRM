﻿using InterlinkMapper.Models;
using System.Data;

namespace InterlinkMapper.Materializer;

internal interface IMaterializer
{
	int CommandTimeout { get; }
}

internal static class IMaterializerExtension
{
	internal static Material CreateMaterial(this IMaterializer materializer, IDbConnection connection, InterlinkTransaction transaction, CreateTableQuery query)
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