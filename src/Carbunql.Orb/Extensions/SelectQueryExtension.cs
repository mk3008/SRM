using Carbunql.Building;
using Carbunql.Clauses;

namespace Carbunql.Orb.Extensions;

public static class SelectQueryExtension
{
	public static (SelectableTable Table, TypeMap Map) AddInnerJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition, string propName)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, propName, from => from.InnerJoin(joinToDefinition.GetTableFullName()));
	}

	public static (SelectableTable Table, TypeMap Map) AddLeftJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition, string propName)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, propName, from => from.LeftJoin(joinToDefinition.GetTableFullName()));
	}

	public static (SelectableTable Table, TypeMap Map) AddJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition, string propName, Func<FromClause, Relation> fn)
	{
		var from = sq.FromClause;
		if (from == null) throw new InvalidOperationException();

		var index = from.GetSelectableTables().Count();
		var alias = "t" + index;

		var map = new TypeMap()
		{
			TableAlias = alias,
			Type = joinToDefinition.Type!,
			ColumnMaps = new(),
			RelationMap = new() { OwnerTableAlias = joinFromTable.Alias, OwnerPropertyName = propName },
		};

		var joinToTableName = joinToDefinition.GetTableFullName();
		if (string.IsNullOrEmpty(joinToTableName)) throw new InvalidOperationException();

		var seq = joinToDefinition.GetSequence();
		var table = fn(from).As(alias).On(joinFromTable, seq.ColumnName);

		sq.Select(table, seq.ColumnName).As(alias + "_" + seq.Identifer);
		map.ColumnMaps.Add(new() { ColumnName = alias + "_" + seq.Identifer, PropertyName = seq.Identifer });

		foreach (var column in joinToDefinition.ColumnDefinitions.Where(x => x != seq && x.RelationType == null))
		{
			if (string.IsNullOrEmpty(column.Identifer)) continue;
			sq.Select(table, column.ColumnName).As(alias + "_" + column.Identifer);
			map.ColumnMaps.Add(new() { ColumnName = alias + "_" + column.Identifer, PropertyName = column.Identifer });
		}

		return (table, map);
	}
}
