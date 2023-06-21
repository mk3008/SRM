using Carbunql.Analysis.Parser;
using Carbunql.Building;
using Carbunql.Orb.Extensions;
using Carbunql.Values;
using Cysharp.Text;
using System.Data.Common;

namespace Carbunql.Orb;

public interface IDbTableDefinition : IDbTable
{
	IEnumerable<DbColumnDefinition> ColumnDefinitions { get; }

	List<DbIndexDefinition> Indexes { get; }
}

public static class IDbTableDefinitionExtention
{
	public static string ToCreateTableCommandText(this IDbTableDefinition source)
	{
		var table = ValueParser.Parse(source.GetTableFullName()).ToText();

		var sb = ZString.CreateStringBuilder();
		foreach (var column in source.ColumnDefinitions)
		{
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("    " + column.ToCommandText());
		}

		var pkeys = source.ColumnDefinitions.Where(x => x.IsPrimaryKey).ToList();
		if (pkeys.Any())
		{
			var columnText = string.Join(", ", pkeys.Select(x => ValueParser.Parse(x.ColumnName).ToText()));
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("primary key(" + string.Join(", ", columnText) + ")");
		}

		var ukeys = source.ColumnDefinitions.Where(x => x.IsUniqueKey).ToList();
		if (ukeys.Any())
		{
			var columnText = string.Join(", ", ukeys.Select(x => ValueParser.Parse(x.ColumnName).ToText()));
			if (sb.Length > 0) sb.AppendLine(", ");
			sb.Append("unique(" + string.Join(", ", columnText) + ")");
		}

		var sql = @$"create table if not exists {table} (
{sb}
)";
		return sql;
	}

	public static IEnumerable<string> ToCreateIndexCommandTexts(this IDbTableDefinition source)
	{
		var id = 0;
		foreach (var index in source.Indexes)
		{
			id++;
			yield return index.ToCommandText(source, id);
		}
	}

	public static string GetColumnName(this IDbTableDefinition source, string identifer)
	{
		return source.ColumnDefinitions.Where(x => x.Identifer == identifer).Select(x => x.ColumnName).First();
	}

	public static DbColumnDefinition? GetSequenceOrDefault(this IDbTableDefinition source)
	{
		return source.ColumnDefinitions.Where(x => x.IsAutoNumber).FirstOrDefault();
	}

	public static DbColumnDefinition GetSequence(this IDbTableDefinition source)
	{
		var seq = source.GetSequenceOrDefault();
		if (seq == null) throw new NotSupportedException($"Sequence column not defined in {source.GetTableFullName()}");
		return seq;
	}

	public static List<DbColumnDefinition> GetPrimaryKeys(this IDbTableDefinition source)
	{
		var lst = source.ColumnDefinitions.Where(x => x.IsPrimaryKey && !string.IsNullOrEmpty(x.Identifer)).ToList();
		if (!lst.Any()) throw new NotSupportedException($"Primary key column not defined in {source.GetTableFullName()}");
		return lst;
	}

	//private static SelectQueryMapper<T> CreateSelectQueryMapperAsNew<T>(this DbTableDefinition def)
	//{
	//	var sq = new SelectQuery();
	//	var table = ValueParser.Parse(def.GetTableFullName()).ToText();
	//	var (_, t) = sq.From(table).As("t0");

	//	var seq = def.GetSequence();
	//	sq.Select(t, seq.ColumnName).As("t0_" + seq.Identifer);

	//	foreach (var column in def.ColumnDefinitions.Where(x => x != seq && x.RelationType == null))
	//	{
	//		if (string.IsNullOrEmpty(column.Identifer)) continue;
	//		sq.Select(t, column.ColumnName).As(column.Identifer);
	//	}

	//	var mapper = new SelectQueryMapper<T>() { SelectQuery = sq };
	//	mapper.Types.Add(def.Type!);

	//	return mapper;
	//}

	//public static SelectQueryMapper<T> ToMapper<T>(this DbTableDefinition<T> def)
	//{
	//	return ToSelectMapper<T>((DbTableDefinition)def);
	//}

	//public static SelectQueryMapper<T> ToSelectMapper<T>(this DbTableDefinition def)
	//{
	//	var mapper = def.CreateSelectQueryMapperAsNew<T>();

	//	//TODO
	//	foreach (var column in def.ColumnDefinitions)
	//	{
	//		if (column.RelationType == null) continue;
	//		var subdef = ObjectRelationMapper.FindFirst(column.RelationType);
	//		var subseq = subdef.GetSequence();

	//		mapper.Types.Add(subdef.Type!);
	//		mapper.SplitOn.Add(subseq.Identifer);

	//		var from = mapper.SelectQuery.FromClause!.Root;
	//		if (!column.AllowNull)
	//		{
	//			var st = mapper.SelectQuery.AddInnerJoin(from, subdef);
	//		}
	//		else
	//		{
	//			var st = mapper.SelectQuery.AddLeftJoin(from, subdef);
	//		}
	//	}

	//	return mapper;
	//}

	public static (InsertQuery Query, DbColumnDefinition? Sequence) ToInsertQuery<T>(this IDbTableDefinition source, T instance, string placeholderIdentifer)
	{
		var seq = source.GetSequenceOrDefault();

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in source.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			if (item == seq && pv.Value == null) continue;
			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		var query = vq.ToSelectQuery(cols).ToInsertQuery(source.GetTableFullName());

		if (seq != null) query.Returning(new ColumnValue(seq.ColumnName));

		return (query, seq);
	}

	public static UpdateQuery ToUpdateQuery<T>(this IDbTableDefinition source, T instance, string placeholderIdentifer)
	{
		var pkeys = source.GetPrimaryKeys();

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in source.ColumnDefinitions)
		{
			if (string.IsNullOrEmpty(item.Identifer)) continue;

			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		return vq.ToSelectQuery(cols).ToUpdateQuery(source.GetTableFullName(), pkeys.Select(x => x.ColumnName));
	}

	public static DeleteQuery ToDeleteQuery<T>(this IDbTableDefinition source, T instance, string placeholderIdentifer)
	{
		var pkeys = source.GetPrimaryKeys();

		var row = new ValueCollection();
		var cols = new List<string>();

		foreach (var item in pkeys)
		{
			var prop = item.Identifer.ToPropertyInfo<T>();
			var pv = prop.ToParameterValue(instance, placeholderIdentifer);

			row.Add(pv);
			cols.Add(item.ColumnName);
		}

		var vq = new ValuesQuery(new List<ValueCollection>() { row });
		return vq.ToSelectQuery(cols).ToDeleteQuery(source.GetTableFullName());
	}
}
