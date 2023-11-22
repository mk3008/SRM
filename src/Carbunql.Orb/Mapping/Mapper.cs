using System.Collections;
using System.Data;
using Utf8Json;

namespace Carbunql.Orb.Mapping;

internal class Mapper : IList<InstanceMap>
{
	private readonly List<InstanceMap> Collection = new();

	internal InstanceCacheRepository Repository { get; init; } = new();

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
		var tp = instanceMap.GetInstanceType();

		var keys = new List<object>();
		foreach (var columnmap in instanceMap.GetPrimaryKeyColumnMap())
		{
			var prop = tp.GetProperty(columnmap.PropertyName)!;
			var val = r[columnmap.ColumnName];

			if (val == null)
			{
				instanceMap.Item = null;
				break;
			}

			// TODO: Custom mapping
			prop.SetValue(instanceMap.Item, val);
		}

		if (instanceMap.Item == null) return;

		var key = JsonSerializer.ToJsonString(keys);
		var cash = Repository.LoadOrDefault(tp, key);
		if (cash != null)
		{
			instanceMap.Item = cash;
			return;
		}
		Repository.Save(tp, key, instanceMap.Item);

		foreach (var columnmap in instanceMap.GetSubordinationColumnMap())
		{
			var prop = tp.GetProperty(columnmap.PropertyName)!;
			var val = r[columnmap.ColumnName];

			// TODO: Custom mapping
			prop.SetValue(instanceMap.Item, val);
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
