namespace InterlinkMapper.Materializer;

public class MaterializeResult
{
	public SelectQuery SelectQuery { get; init; } = null!;

	public int Count { get; set; }

	public string MaterialName { get; init; } = string.Empty;
}
