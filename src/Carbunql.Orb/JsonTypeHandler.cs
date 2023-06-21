using System.Data;
using System.Reflection;
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

public class ObjectTableMappableTypeHandler<T> : TypeHandler<T?>
{
	public ObjectTableMappableTypeHandler()
	{
		var def = ObjectTableMapper.FindFirst<T>();
		var seq = def.GetSequence();
		var prop = typeof(T).GetProperty(seq.Identifer);
		if (prop == null) throw new NotSupportedException($"'{seq.Identifer}' property not found in '{typeof(T).FullName}' class");
		SequenceProperty = prop;
	}

	private PropertyInfo SequenceProperty { get; init; }

	public override T? Parse(object value)
	{
		if (value == null || value is DBNull) return default;

		var instance = Activator.CreateInstance<T>();
		SequenceProperty.SetValue(instance, value);
		return instance;
	}

	public override void SetValue(IDbDataParameter parameter, T? value)
	{
		var def = ObjectTableMapper.FindFirst<T>();
		var seq = def.GetSequence();
		if (value == null)
		{
			parameter.Value = null;
		}
		else
		{
			parameter.Value = SequenceProperty.GetValue(value);
		}
	}
}