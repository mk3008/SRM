using InterlinkMapper.Models;
using PrivateProxy;

namespace InterlinkMapper.Materializer;

public abstract class MaterializeResult
{
	public required SelectQuery SelectQuery { get; init; }

	public required int Count { get; init; }

	public required string MaterialName { get; init; }

	public required string PlaceHolderIdentifer { get; init; }

	public required string InterlinkTransactionIdColumn { get; init; }

	public required string InterlinkProcessIdColumn { get; init; }


	public required string InterlinkDestinationIdColumn { get; init; }

	public required string InterlinkDatasourceIdColumn { get; init; }

	/// <summary>
	/// Master ID of datasource table
	/// ex."interlink_datasource_id"
	/// </summary>
	public required long InterlinkDatasourceId { get; set; }

	/// <summary>
	/// Master ID of destination table
	/// ex."interlink_destination_id"
	/// </summary>
	public required long InterlinkDestinationId { get; set; }

	public required string InterlinkRelationTable { get; init; }

	public required string DestinationTable { get; init; }

	/// <summary>
	/// Column name that stores forwarding destination ID
	/// ex."sale_id", "shop_id" etc
	/// </summary>
	public required string DestinationIdColumn { get; init; }

	public required List<string> DestinationColumns { get; init; }

	public required string RootIdColumn { get; init; }

	public required string OriginIdColumn { get; init; }

	public required string InterlinkRemarksColumn { get; init; }

	public required int CommandTimeout { get; init; }

	public required string KeyRelationTable { get; init; }

	public required List<string> DatasourceKeyColumns { get; init; }

	public required string KeyMapTableNameColumn { get; init; }

	public required string KeyRelationTableNameColumn { get; init; }

	public required string ActionColumn { get; init; }

	public required string InsertCountColumn { get; init; }

	public required string ProcessTableName { get; init; }


	internal InsertQuery CreateProcessInsertQuery(InterlinkProcessRow row)
	{
		//select :transaction_id, :destination_name, :datasoruce_name, :keymap_table, :relationmap_table
		var sq = new SelectQuery();
		sq.Select(PlaceHolderIdentifer, InterlinkTransactionIdColumn, row.InterlinkTransactionId);
		sq.Select(PlaceHolderIdentifer, InterlinkDatasourceIdColumn, row.InterlinkDatasourceId);
		sq.Select(PlaceHolderIdentifer, InterlinkDestinationIdColumn, row.InterlinkDestinationId);
		sq.Select(PlaceHolderIdentifer, KeyMapTableNameColumn, row.KeyMapTableName);
		sq.Select(PlaceHolderIdentifer, KeyRelationTableNameColumn, row.KeyRelationTableName);
		sq.Select(PlaceHolderIdentifer, ActionColumn, row.ActionName);
		sq.Select(PlaceHolderIdentifer, InsertCountColumn, row.InsertCount);

		//insert into process_table returning process_id
		var iq = sq.ToInsertQuery(ProcessTableName);
		iq.Returning(InterlinkProcessIdColumn);

		return iq;
	}

	//internal InsertQuery CreateRelationInsertQuery(long processId)
	//{
	//	var sq = CreateRelationInsertSelectQuery(processId);
	//	return sq.ToInsertQuery(RelationTable);
	//}

	internal SelectQuery CreateRelationInsertSelectQuery(long processId)
	{
		var sq = new SelectQuery();
		var ct = sq.With(SelectQuery).As("d");

		var (f, d) = sq.From(ct).As("d");
		var kr = f.LeftJoin(CreateFirstKeyRelationSelectQuery()).As("kr").On(d, DatasourceKeyColumns);

		sq.Select(PlaceHolderIdentifer, InterlinkProcessIdColumn, processId);
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
		if (ct.GetColumnNames().Where(x => x.IsEqualNoCase(InterlinkRemarksColumn)).Any())
		{
			sq.Select(d, InterlinkRemarksColumn);
		}
		else
		{
			sq.Select("null").As(InterlinkRemarksColumn);
		}

		return sq;
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