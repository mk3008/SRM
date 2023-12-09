﻿using PrivateProxy;

namespace InterlinkMapper.Materializer;

public abstract class MaterializeResult
{
	public required SelectQuery SelectQuery { get; init; } = null!;

	public required int Count { get; init; }

	public required string MaterialName { get; init; } = string.Empty;

	public required string PlaceHolderIdentifer { get; init; }

	public required string ProcessIdColumn { get; init; }

	public required string DestinationTable { get; init; }

	public required string DestinationIdColumn { get; init; }

	public required List<string> DestinationColumns { get; init; }

	public required string RelationTable { get; init; }

	public required string RootIdColumn { get; init; }

	public required string OriginIdColumn { get; init; }

	public required string RemarksColumn { get; init; }

	public required string ReverseTable { get; init; }

	public required int CommandTimeout { get; init; }

	public required string KeyRelationTable { get; init; }

	public required List<string> DatasourceKeyColumns { get; init; }

	internal InsertQuery CreateRelationInsertQuery(long processId)
	{
		//var sq = new SelectQuery();
		//var (_, d) = sq.From(SelectQuery).As("d");

		//sq.Select(d, DestinationIdColumn);
		//sq.Select(PlaceHolderIdentifer, ProcessIdColumn, processId);

		//return sq.ToInsertQuery(RelationTable);

		var sq = new SelectQuery();
		var ct = sq.With(SelectQuery).As("d");

		var (f, d) = sq.From(ct).As("d");
		var kr = f.LeftJoin(CreateFirstKeyRelationSelectQuery()).As("kr").On(d, DatasourceKeyColumns);

		sq.Select(PlaceHolderIdentifer, ProcessIdColumn, processId);
		sq.Select(d, DestinationIdColumn);
		sq.Select(GetCoalesceValue(new ColumnValue(kr, RootIdColumn), new ColumnValue(d, DestinationIdColumn))).As(RootIdColumn);

		if (ct.GetColumnNames().Where(x => x.IsEqualNoCase(OriginIdColumn)).Any())
		{
			sq.Select(d, OriginIdColumn);
		}
		else
		{
			// Record this document as an original document
			sq.Select(d, DestinationIdColumn).As(OriginIdColumn);
		}
		if (ct.GetColumnNames().Where(x => x.IsEqualNoCase(RemarksColumn)).Any())
		{
			sq.Select(d, RemarksColumn);
		}
		else
		{
			sq.Select("null").As(RemarksColumn);
		}

		return sq.ToInsertQuery(RelationTable);
	}

	internal InsertQuery CreateDestinationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d);

		// Exclude from selection if it does not exist in the destination column
		sq.SelectClause!.FilterInColumns(DestinationColumns);

		return sq.ToInsertQuery(DestinationTable);
	}

	private ValueBase GetCoalesceValue(params ValueBase[] values)
	{
		var args = new ValueCollection(values.ToList());
		return new FunctionValue("coalesce", args);
	}

	private SelectQuery CreateFirstKeyRelationSelectQuery()
	{
		var sq = new SelectQuery();
		sq.AddComment("if reverse transfer is performed, one or more rows exist.");

		var (_, kr) = sq.From(CreateKeyRelationSelectQuery()).As("kr");
		DatasourceKeyColumns.ForEach(key => sq.Select(kr, key));
		sq.Select(kr, DestinationIdColumn).As(RootIdColumn);
		sq.Where(kr, "_row_num").Equal(1);
		return sq;
	}

	private SelectQuery CreateKeyRelationSelectQuery()
	{
		var sq = new SelectQuery();
		var ct = sq.With(SelectQuery).As("d");
		var (f, kr) = sq.From(KeyRelationTable).As("kr");
		var d = f.InnerJoin(ct).As("d").On(kr, DatasourceKeyColumns);

		var dskeys = new ValueCollection();
		DatasourceKeyColumns.ForEach(key => dskeys.Add(new ColumnValue(kr, key)));

		var over = new OverClause();
		over.AddPartition(dskeys);
		over.AddOrder(new SortableItem(new ColumnValue(kr, DestinationIdColumn)));

		DatasourceKeyColumns.ForEach(key => sq.Select(kr, key));
		sq.Select(kr, DestinationIdColumn);
		sq.Select(new FunctionValue("row_number", over)).As("_row_num");

		return sq;
	}
}

[GeneratePrivateProxy(typeof(MaterializeResult))]
public partial struct MaterializeResultProxy;