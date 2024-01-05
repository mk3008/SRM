namespace InterlinkMapper.Models;

public class SystemEnvironment
{
	public IDbConnetionSetting DbConnetionConfig { get; set; } = null!;

	public DbTableConfig DbTableConfig { get; set; } = new();

	public DbEnvironment DbEnvironment { get; set; } = new();
}
