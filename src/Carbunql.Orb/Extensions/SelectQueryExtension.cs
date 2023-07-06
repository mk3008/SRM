using Carbunql.Building;
using Carbunql.Clauses;
using Carbunql.Values;

namespace Carbunql.Orb.Extensions;

public static class SelectQueryExtension
{
	public static void AddSelectPrimarykeyColumns(this SelectQuery sq, DbTableDefinition def, TypeMap map)
	{
		var t = sq.FromClause!.GetSelectableTables().Where(x => x.Alias == map.TableAlias).First();

		var pkeys = def.GetPrimaryKeys();
		pkeys.ForEach(column =>
		{
			var name = map.TableAlias + column.Identifer;
			sq.Select(t, column.ColumnName).As(name);
			map.ColumnMaps.Add(new() { ColumnName = name, PropertyName = column.Identifer });
		});
	}

	public static void AddSelectColumnsWithoutPrimaryKeys(this SelectQuery sq, DbTableDefinition def, TypeMap map)
	{
		var t = sq.FromClause!.GetSelectableTables().Where(x => x.Alias == map.TableAlias).First();

		var pkeys = def.GetPrimaryKeys();
		def.ColumnDefinitions.Where(x => !string.IsNullOrEmpty(x.Identifer) && !pkeys.Contains(x)).ToList().ForEach(column =>
		{
			var name = map.TableAlias + column.Identifer;
			sq.Select(t, column.ColumnName).As(name);
			map.ColumnMaps.Add(new() { ColumnName = name, PropertyName = column.Identifer });
		});
	}

	public static List<TypeMap> AddJoin(this SelectQuery sq, DbParentRelationDefinition relation, TypeMap fromMap, ICascadeRule rule)
	{
		var destination = ObjectRelationMapper.FindFirst(relation.IdentiferType);
		bool isNullable = Nullable.GetUnderlyingType(relation.IdentiferType) != null;

		var fromKeys = relation.ColumnNames;
		var toKeys = destination.GetPrimaryKeys();
		var joinType = (isNullable) ? "left join" : "inner join";

		var index = sq.FromClause!.GetSelectableTables().Count();

		var map = new TypeMap()
		{
			TableAlias = "t" + index,
			Type = relation.IdentiferType,
			ColumnMaps = new(),
			RelationMap = new() { OwnerTableAlias = fromMap.TableAlias, OwnerPropertyName = relation.Identifer },
		};
		var maps = new List<TypeMap>() { map };

		var t = sq.FromClause!.Join(destination.SchemaName, destination.TableName, joinType).As("t" + index).On(x =>
		{
			ValueBase? condition = null;
			for (int i = 0; i < fromKeys.Count(); i++)
			{
				if (condition == null)
				{
					condition = new ColumnValue(fromMap.TableAlias, fromKeys[i]);
				}
				else
				{
					condition.And(fromMap.TableAlias, fromKeys[i]);
				}
				condition.Equal(x.Table.Alias, toKeys[i].ColumnName);
			}
			if (condition == null) throw new InvalidOperationException();
			return condition;
		});

		sq.AddSelectPrimarykeyColumns(destination, map);
		sq.AddSelectColumnsWithoutPrimaryKeys(destination, map);

		destination.ParentRelations.Where(x => rule.DoRelation(destination.Type!, x.IdentiferType)).ToList().ForEach(relation =>
		{
			maps.AddRange(sq.AddJoin(relation, map, rule));
		});

		return maps;
	}
}
