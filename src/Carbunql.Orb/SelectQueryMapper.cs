using Carbunql.Dapper;
using Carbunql.Orb.Mapping;
using System.Data;

namespace Carbunql.Orb;

public class SelectQueryMapper<T>
{
	public SelectQueryMapper(ICascadeRule? rule = null)
	{
		var def = ObjectRelationMapper.FindFirst<T>();
		var val = def.ToSelectQueryMap<T>();

		rule ??= new FullCascadeRule();

		SelectQuery = val.Query;
		TypeMaps = val.Maps;
	}

	private SelectQuery SelectQuery { get; init; }

	private List<TypeMap> TypeMaps { get; init; }

	public List<T> Load(IDbConnection cn)
	{
		var lst = new List<T>();

		using var r = cn.ExecuteReader(SelectQuery);

		while (r.Read())
		{
			var mapper = CreateMapper();
			var root = mapper.Execute(r);
			if (root == null) continue;
			lst.Add((T)root);
		}

		return lst;
	}

	private Mapper CreateMapper()
	{
		var lst = new Mapper();
		foreach (var map in TypeMaps)
		{
			lst.Add(new() { TypeMap = map, Item = Activator.CreateInstance(map.Type)! });
		}
		return lst;
	}
}
