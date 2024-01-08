using InterlinkMapper.Models;
using PrivateProxy;

namespace InterlinkMapper.Materializer;

public abstract class DatasourceMaterial
{
	public required SelectQuery SelectQuery { get; set; }

	public required string MaterialName { get; set; }

	public required string PlaceHolderIdentifer { get; set; }

	//public required string InterlinkTransactionIdColumn { get; init; }

	public required InterlinkTransaction InterlinkTransaction { get; set; }

	public required SystemEnvironment Environment { get; set; }

	public required string InterlinkProcessIdColumn { get; set; }

	public required string InterlinkDatasourceIdColumn { get; set; }

	/// <summary>
	/// Master ID of datasource table
	/// ex."interlink_datasource_id"
	/// </summary>
	//public required long InterlinkDatasourceId { get; set; }

	/// <summary>
	/// Master ID of destination table
	/// ex."interlink_destination_id"
	/// </summary>
	//public required long InterlinkDestinationId { get; set; }

	public required string InterlinkRelationTable { get; set; }

	public required string DestinationTable { get; set; }

	/// <summary>
	/// Column name that stores forwarding destination ID
	/// ex."sale_id", "shop_id" etc
	/// </summary>
	public required string DestinationIdColumn { get; set; }

	public required List<string> DestinationColumns { get; set; }

	public required string RootIdColumn { get; set; }

	public required string OriginIdColumn { get; set; }

	public required string InterlinkRemarksColumn { get; set; }

	public required int CommandTimeout { get; set; }

	private string CteName { get; set; } = "material_data";

	private string RowNumberColumnName { get; set; } = "row_num";

	internal InsertQuery CreateRelationInsertQuery(long processId, string keyRelationTable, List<string> datasourceKeyColumns)
	{
		var sq = CreateRelationInsertSelectQuery(processId, keyRelationTable, datasourceKeyColumns);
		return sq.ToInsertQuery(InterlinkRelationTable);
	}

	internal SelectQuery CreateRelationInsertSelectQuery(long processId, string keyRelationTable, List<string> datasourceKeyColumns)
	{
		var sq = new SelectQuery();
		var ct = sq.With(SelectQuery).As(CteName);

		var (f, d) = sq.From(ct).As("d");
		var kr = f.LeftJoin(CreateFirstKeyRelationSelectQuery(keyRelationTable, datasourceKeyColumns)).As("kr").On(d, datasourceKeyColumns);

		sq.Select(processId.ToString()).As(InterlinkProcessIdColumn);
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
			sq.Select("''").As(InterlinkRemarksColumn);
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

	private SelectQuery CreateFirstKeyRelationSelectQuery(string keyRelationTable, List<string> datasourceKeyColumns)
	{
		var sq = new SelectQuery();
		sq.AddComment("if reverse transfer is performed, one or more rows exist.");

		var (_, kr) = sq.From(CreateKeyRelationSelectQuery(keyRelationTable, datasourceKeyColumns)).As("kr");
		datasourceKeyColumns.ForEach(key => sq.Select(kr, key));
		sq.Select(kr, DestinationIdColumn).As(RootIdColumn);
		sq.Where(kr, RowNumberColumnName).Equal(1);
		return sq;
	}

	private SelectQuery CreateKeyRelationSelectQuery(string keyRelationTable, List<string> datasourceKeyColumns)
	{
		var sq = new SelectQuery();
		var ct = sq.With(SelectQuery).As(CteName);
		var (f, kr) = sq.From(keyRelationTable).As("kr");
		var d = f.InnerJoin(ct).As("d").On(kr, datasourceKeyColumns);

		var dskeys = new ValueCollection();
		datasourceKeyColumns.ForEach(key => dskeys.Add(new ColumnValue(kr, key)));

		var over = new OverClause();
		over.AddPartition(dskeys);
		over.AddOrder(new SortableItem(new ColumnValue(kr, DestinationIdColumn)));

		datasourceKeyColumns.ForEach(key => sq.Select(kr, key));
		sq.Select(kr, DestinationIdColumn);
		sq.Select(new FunctionValue("row_number", over)).As(RowNumberColumnName);

		return sq;
	}
}

[GeneratePrivateProxy(typeof(DatasourceMaterial))]
public partial struct DatasourceMaterialProxy;