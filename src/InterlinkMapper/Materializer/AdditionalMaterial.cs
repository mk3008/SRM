using Carbunql.Building;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalMaterial : MaterializeResult
{
	public required string KeyMapTable { get; init; }



	//public required string KeyRelationIdColumn { get; init; }



	public required string NumericType { get; init; }

	internal InsertQuery CreateKeyMapInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyMapTable);
	}

	internal InsertQuery CreateKeyRelationInsertQuery()
	{
		var sq = new SelectQuery();
		var (_, d) = sq.From(SelectQuery).As("d");

		sq.Select(d, DestinationIdColumn);
		DatasourceKeyColumns.ForEach(key => sq.Select(d, key));

		return sq.ToInsertQuery(KeyRelationTable);
	}

	//internal InsertQuery CreateKeyRelationInsertQuery()
	//{
	//	var sq = new SelectQuery();
	//	var ct = sq.With(SelectQuery).As("d");

	//	var (f, d) = sq.From(ct).As("d");
	//	var kr = f.LeftJoin(CreateFirstKeyRelationSelectQuery()).As("kr").On(d, DatasourceKeyColumns);

	//	DatasourceKeyColumns.ForEach(key => sq.Select(d, key));
	//	sq.Select(d, DestinationIdColumn);
	//	sq.Select(GetCoalesceValue(new ColumnValue(kr, RootIdColumn), new ColumnValue(d, DestinationIdColumn))).As(RootIdColumn);
	//	sq.Select(GetNullValueAsNumeric).As(OriginIdColumn);
	//	sq.Select(d, RemarksColumn);

	//	return sq.ToInsertQuery(KeyRelationTable);
	//}

	//private SelectQuery CreateFirstKeyRelationSelectQuery()
	//{
	//	var sq = new SelectQuery();
	//	sq.AddComment("if reverse transfer is performed, one or more rows exist.");

	//	var (_, kr) = sq.From(CreateKeyRelationSelectQuery()).As("kr");
	//	DatasourceKeyColumns.ForEach(key => sq.Select(kr, key));
	//	sq.Select(kr, RootIdColumn);
	//	sq.Where(kr, "_row_num").Equal(1);
	//	return sq;
	//}

	//private SelectQuery CreateKeyRelationSelectQuery()
	//{
	//	var sq = new SelectQuery();
	//	var ct = sq.With(SelectQuery).As("d");
	//	var (f, kr) = sq.From(KeyRelationTable).As("kr");
	//	var d = f.InnerJoin(ct).As("d").On(kr, DatasourceKeyColumns);

	//	var dskeys = new ValueCollection();
	//	DatasourceKeyColumns.ForEach(key => dskeys.Add(new ColumnValue(kr, key)));

	//	var over = new OverClause();
	//	over.AddPartition(dskeys);
	//	over.AddOrder(new SortableItem(new ColumnValue(kr, DestinationIdColumn)));

	//	DatasourceKeyColumns.ForEach(key => sq.Select(kr, key));
	//	sq.Select(kr, RootIdColumn);
	//	sq.Select(new FunctionValue("row_number", over)).As("_row_num");

	//	return sq;
	//}

	public void ExecuteTransfer(IDbConnection connection, long processId)
	{
		// transfer datasource
		connection.Execute(CreateRelationInsertQuery(processId), commandTimeout: CommandTimeout);
		connection.Execute(CreateDestinationInsertQuery(), commandTimeout: CommandTimeout);

		// create system relation mapping
		connection.Execute(CreateKeyMapInsertQuery(), commandTimeout: CommandTimeout);
		connection.Execute(CreateKeyRelationInsertQuery(), commandTimeout: CommandTimeout);
	}


}

[GeneratePrivateProxy(typeof(AdditionalMaterial))]
public partial struct AdditionalMaterialProxy;