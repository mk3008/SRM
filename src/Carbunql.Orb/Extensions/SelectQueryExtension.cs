using Carbunql.Building;
using Carbunql.Clauses;

namespace Carbunql.Orb.Extensions;

public static class SelectQueryExtension
{
	public static SelectableTable AddInnerJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, from => from.InnerJoin(joinToDefinition.GetTableFullName()));
	}

	public static SelectableTable AddLeftJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, from => from.LeftJoin(joinToDefinition.GetTableFullName()));
	}

	public static SelectableTable AddJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition, Func<FromClause, Relation> fn)
	{
		var joinToTableName = joinToDefinition.GetTableFullName();
		if (string.IsNullOrEmpty(joinToTableName)) throw new InvalidOperationException();

		var seq = joinToDefinition.GetSequence();

		var from = sq.FromClause;
		if (from == null) throw new InvalidOperationException();

		var index = from.GetSelectableTables().Count();
		var alias = "t" + index;
		var t = fn(from).As(alias).On(joinFromTable, seq.ColumnName);

		sq.Select(t, seq.ColumnName).As(alias + "_" + seq.Identifer);

		foreach (var column in joinToDefinition.ColumnDefinitions.Where(x => x != seq && x.RelationType == null))
		{
			if (string.IsNullOrEmpty(column.Identifer)) continue;
			sq.Select(t, column.ColumnName).As(alias + "_" + column.Identifer);
		}

		return t;
	}
}
