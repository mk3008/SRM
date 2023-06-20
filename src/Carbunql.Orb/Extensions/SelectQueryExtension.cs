using Carbunql.Building;
using Carbunql.Clauses;

namespace Carbunql.Orb.Extensions;

public static class SelectQueryExtension
{
	public static SelectableTable AddInnerJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, from => from.InnerJoin(joinFromTable));
	}

	public static SelectableTable AddLeftJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition)
	{
		return sq.AddJoin(joinFromTable, joinToDefinition, from => from.LeftJoin(joinFromTable));
	}

	public static SelectableTable AddJoin(this SelectQuery sq, SelectableTable joinFromTable, IDbTableDefinition joinToDefinition, Func<FromClause, Relation> fn)
	{
		var joinToTableName = joinToDefinition.GetTableFullName();
		if (string.IsNullOrEmpty(joinToTableName)) throw new InvalidOperationException();

		var pkeys = joinToDefinition.GetPrimaryKeys().Select(x => x.ColumnName);
		if (!pkeys.Any()) throw new InvalidOperationException();

		var from = sq.FromClause;
		if (from == null) throw new InvalidOperationException();

		var index = from.GetSelectableTables().Count();
		var t = fn(from).As("t" + index).On(joinFromTable, pkeys);

		foreach (var column in joinToDefinition.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(column.Identifer)) continue;
			sq.Select(t, column.ColumnName).As(column.Identifer);
		}

		return t;
	}
}
