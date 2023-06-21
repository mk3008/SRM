namespace Carbunql.Orb;

public class DbTableDefinition : IDbTableDefinition
{
	public string SchemaName { get; init; } = string.Empty;

	public required string TableName { get; init; }

	public string Comment { get; set; } = string.Empty;

	public List<DbColumnDefinition> ColumnDefinitions { get; init; } = new();

	public IEnumerable<string> ColumnNames => ColumnDefinitions.Select(x => x.ColumnName);

	IEnumerable<DbColumnDefinition> IDbTableDefinition.ColumnDefinitions => ColumnDefinitions;

	public List<DbIndexDefinition> Indexes { get; init; } = new();
}

public class DbTableDefinition<T> : DbTableDefinition
{
	public Type Type { get; } = typeof(T);
}