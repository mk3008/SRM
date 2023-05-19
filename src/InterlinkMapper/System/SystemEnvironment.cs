namespace InterlinkMapper.System;

public class SystemEnvironment
{
	public IDbConnetionConfig DbConnetionConfig { get; set; }

	public DbTableConfig DbTableConfig { get; set; }

	public DbQueryConfig DbQueryConfig { get; set; }
}
