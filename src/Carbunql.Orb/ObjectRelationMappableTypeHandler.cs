using System.Data;
using System.Reflection;
using static Dapper.SqlMapper;

namespace Carbunql.Orb;

public class ObjectRelationMappableTypeHandler<T> : TypeHandler<T?>
{
	public ObjectRelationMappableTypeHandler()
	{
		var def = ObjectRelationMapper.FindFirst<T>();
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
		var def = ObjectRelationMapper.FindFirst<T>();
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
