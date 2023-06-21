using System.Data;
using Utf8Json;
using static Dapper.SqlMapper;

namespace Carbunql.Orb;

public class JsonTypeHandler<T> : TypeHandler<T?>
{
	public override T? Parse(object value)
	{
		if (value == null || value is DBNull) return default;
		return JsonSerializer.Deserialize<T?>(value.ToString());
	}

	public override void SetValue(IDbDataParameter parameter, T? value)
	{
		if (value == null)
		{
			parameter.Value = null;
		}
		else
		{
			parameter.Value = JsonSerializer.ToJsonString(value);
		}
	}
}
