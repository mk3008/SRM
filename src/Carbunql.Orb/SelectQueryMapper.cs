//namespace Carbunql.Orb;

//public class SelectQueryMapper<T>
//{
//	public required SelectQuery SelectQuery { get; init; }

//	public List<Type> Types { get; init; } = new();

//	public List<string> SplitOn { get; init; } = new();

//	public T Mapping(object[] items)
//	{
//		var root = (T)items[0];
//		var relationItems = items.Where(x => x != items[0]).ToList();
//		List<(Type Type, object Item)> relations = relationItems.Select(x => (x.GetType(), x)).ToList();

//		var usedIndex = new Dictionary<Type, int>();
//		foreach (var current in relationItems)
//		{
//			if (usedIndex.TryGetValue(current.GetType(), out var index))
//			{
//				relationItems.Where(x => x.GetType() == items[1].GetType()).Count();
//			}
//		}



//		var count = relationItems.Where(x => x.GetType() == items[1].GetType()).Count();
//		items[1]

//		return root;
//	}
//}
