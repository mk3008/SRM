using InterlinkMapper.Models;

namespace InterlinkMapper.Materializer;

public class Material
{
	public required SelectQuery SelectQuery { get; set; }

	public required InterlinkTransaction InterlinkTransaction { get; set; }

	public required int Count { get; set; }

	public required string MaterialName { get; set; }
}
