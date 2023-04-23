using System.Data.Common;
using System.Data;

namespace InterlinkMapper.Services;

public class ConfigService
{
	public ConfigService(IDbConnection cn)
	{
		Connection = cn;
	}

	public List<string> ConfigName { get; init; } = new() { "datasource_table_definition", "" };

	private IDbConnection Connection { get; init; }

	public string PlaceholderIdentifier { get; init; }
}
