namespace InterlinkMapper.Materializer;

public class Material
{
	public required SelectQuery SelectQuery { get; init; } = null!;

	public required int Count { get; init; }

	public required string MaterialName { get; init; } = string.Empty;
}
