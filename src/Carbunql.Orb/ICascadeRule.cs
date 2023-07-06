namespace Carbunql.Orb;

public interface ICascadeRule
{
	bool DoRelation(Type from, Type to);
}

public class FullCascadeRule : ICascadeRule
{
	public bool DoRelation(Type from, Type to)
	{
		return true;
	}
}

public class TierCascadeRule : ICascadeRule
{
	public TierCascadeRule(Type rootType)
	{
		TypeTiers.Add(rootType, 0);
	}

	private Dictionary<Type, int> TypeTiers { get; set; } = new();

	public int UpperTier { get; set; } = 0;

	public bool DoRelation(Type from, Type to)
	{
		if (!TypeTiers.ContainsKey(from)) return false;
		var fromTier = TypeTiers[from];

		if (UpperTier < fromTier) return false;

		if (TypeTiers.ContainsKey(to)) return false;
		TypeTiers[to] = fromTier + 1;

		return true;
	}
}

public class CascadeRule : ICascadeRule
{
	public List<CascadeRelation> CascadeRelationRules { get; set; } = new();

	public bool DoRelation(Type from, Type to)
	{
		return (CascadeRelationRules.Where(x => x.FromType.Equals(from) && x.ToType.Equals(to)).Any());
	}
}

public class CascadeRelation
{
	public required Type FromType { get; set; }
	public required Type ToType { get; set; }
}