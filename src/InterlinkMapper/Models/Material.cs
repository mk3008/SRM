namespace InterlinkMapper.Models;

/// <summary>
/// The material of the query result.
/// Synonymous with temporary table.
/// </summary>
public class Material
{
	/// <summary>
	/// Temporary table select query.
	/// </summary>
	public required SelectQuery SelectQuery { get; init; }

	/// <summary>
	/// InterlinkTransaction that generated the material.
	/// </summary>
	public required InterlinkTransaction InterlinkTransaction { get; init; }

	/// <summary>
	/// Number of rows stored.
	/// </summary>
	public required int Count { get; init; }

	/// <summary>
	/// Temporary table name.
	/// </summary>
	public required string MaterialName { get; init; }
}
