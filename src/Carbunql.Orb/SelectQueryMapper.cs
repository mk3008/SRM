using Carbunql.Dapper;
using System.Collections;
using System.Data;
using static Dapper.SqlMapper;

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
		var cash = new Dictionary<(Type, long), object>();

		Func<(Type, long), object, object?> findOrCash = (key, instance) =>
		{
			if (cash.ContainsKey(key)) return cash[key];
			cash[key] = instance;
			return null;
		};

		var lst = new List<T>();

		using var r = cn.ExecuteReader(SelectQuery);

		while (r.Read())
		{
			var mapper = CreateMapper(findOrCash);
			var root = mapper.Execute(r);
			if (root == null) continue;
			lst.Add((T)root);
		}

		return lst;
	}

	private Mapper CreateMapper(Func<(Type, long), object, object?> findOrCash)
	{
		var lst = new Mapper() { FindOrCash = findOrCash };
		foreach (var map in TypeMaps)
		{
			lst.Add(new() { TypeMap = map, Item = Activator.CreateInstance(map.Type)! });
		}
		return lst;
	}
}


public class TypeMap
{
	public required string TableAlias { get; set; }

	public required Type Type { get; set; }

	public RelationMap? RelationMap { get; set; }

	public List<ColumnMap> ColumnMaps { get; set; } = new();
}

public class RelationMap
{
	public required string OwnerTableAlias { get; set; }

	public required string OwnerPropertyName { get; set; }
}

public class ColumnMap
{
	public required string PropertyName { get; set; }

	public required string ColumnName { get; set; }
}

public class InstanceMap
{
	public required TypeMap TypeMap { get; set; }

	public object? Item { get; set; }

	public string TableAlias => TypeMap.TableAlias;
}

public class Mapper : IList<InstanceMap>
{
	private readonly List<InstanceMap> Collection = new();

	public required Func<(Type, long), object, object?> FindOrCash { get; init; }

	public object? Execute(IDataReader r)
	{
		SetObjectMapping(r);
		SetRelationMapping();
		return Collection.Where(x => x.TypeMap.RelationMap == null).Select(x => x.Item).First();
	}

	private void SetObjectMapping(IDataReader r)
	{
		foreach (var item in Collection) SetObjectMapping(r, item);
	}

	private void SetObjectMapping(IDataReader r, InstanceMap instanceMap)
	{
		var tp = instanceMap.TypeMap.Type;
		var seqPropName = ObjectRelationMapper.FindFirst(tp).GetSequence().Identifer;
		var seqProp = tp.GetProperty(seqPropName)!;

		foreach (var columnmap in instanceMap.TypeMap.ColumnMaps)
		{
			var prop = tp.GetProperty(columnmap.PropertyName)!;
			var val = r[columnmap.ColumnName];

			// TODO: Custom mapping
			prop.SetValue(instanceMap.Item, val);

			if (prop != seqProp) continue;

			// Stop mapping if primary key is NULL
			if (val == null)
			{
				instanceMap.Item = null;
				break;
			}

			// Single instance if type and primary key match
			var key = (tp, (long)val);
			var result = FindOrCash(key, instanceMap.Item!);
			if (result != null)
			{
				instanceMap.Item = result;
				break;
			}
		}
	}

	private void SetRelationMapping()
	{
		foreach (var instanceMap in Collection)
		{
			var rmap = instanceMap.TypeMap.RelationMap;
			if (rmap == null) continue;

			var owner = Collection.Where(x => x.TypeMap.TableAlias == rmap.OwnerTableAlias).First();
			if (owner.Item == null) continue;

			var prop = owner.TypeMap.Type.GetProperty(rmap.OwnerPropertyName)!;
			prop.SetValue(owner.Item, instanceMap.Item);
		}
	}

	#region "Ilist"
	public InstanceMap this[int index] { get => ((IList<InstanceMap>)Collection)[index]; set => ((IList<InstanceMap>)Collection)[index] = value; }

	public int Count => ((ICollection<InstanceMap>)Collection).Count;

	public bool IsReadOnly => ((ICollection<InstanceMap>)Collection).IsReadOnly;

	public void Add(InstanceMap item)
	{
		((ICollection<InstanceMap>)Collection).Add(item);
	}

	public void Clear()
	{
		((ICollection<InstanceMap>)Collection).Clear();
	}

	public bool Contains(InstanceMap item)
	{
		return ((ICollection<InstanceMap>)Collection).Contains(item);
	}

	public void CopyTo(InstanceMap[] array, int arrayIndex)
	{
		((ICollection<InstanceMap>)Collection).CopyTo(array, arrayIndex);
	}

	public IEnumerator<InstanceMap> GetEnumerator()
	{
		return ((IEnumerable<InstanceMap>)Collection).GetEnumerator();
	}

	public int IndexOf(InstanceMap item)
	{
		return ((IList<InstanceMap>)Collection).IndexOf(item);
	}

	public void Insert(int index, InstanceMap item)
	{
		((IList<InstanceMap>)Collection).Insert(index, item);
	}

	public bool Remove(InstanceMap item)
	{
		return ((ICollection<InstanceMap>)Collection).Remove(item);
	}

	public void RemoveAt(int index)
	{
		((IList<InstanceMap>)Collection).RemoveAt(index);
	}

	IEnumerator IEnumerable.GetEnumerator()
	{
		return ((IEnumerable)Collection).GetEnumerator();
	}
	#endregion
}
