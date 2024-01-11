using InterlinkMapper.Models;
using InterlinkMapper.Services;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ReverseDatasourceMaterializer : IRequestMaterializer
{
	public ReverseDatasourceMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string MaterialName { get; set; } = "__reverse_datasource";

	private string CteName { get; set; } = "reverse_data";

	public ReverseMaterial Create(IDbConnection connection, InterlinkTransaction transaction, Material request)
	{
		var destination = transaction.InterlinkDestination;

		var query = CreateReverseMaterialQuery(destination, request);
		var reverse = this.CreateMaterial(connection, transaction, query);

		return ToReverseMaterial(reverse);
	}

	private CreateTableQuery CreateReverseMaterialQuery(InterlinkDestination destination, Material request)
	{
		var sq = new SelectQuery();
		var target = sq.With(CreateReverseDatasourceSelectQuery(destination, request)).As(CteName);

		var (f, d) = sq.From(target).As("d");

		sq.Select(destination.DbSequence);
		sq.Select(d);

		return sq.ToCreateTableQuery(MaterialName);
	}

	private SelectQuery CreateReverseDatasourceSelectQuery(InterlinkDestination destination, Material request)
	{
		var relation = destination.GetInterlinkRelationTable(Environment);
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var op = destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("data source to be added");
		var (f, d) = sq.From(destination.ToSelectQuery()).As("d");
		//SelectableTable rm;
		//if (!string.IsNullOrEmpty(request.MaterialName))
		//{
		//	rm = f.InnerJoin(request.MaterialName).As("rm").On(d, destination.DbSequence.ColumnName);
		//}
		//else
		//{
		//	rm = f.InnerJoin(request.SelectQuery).As("rm").On(d, destination.DbSequence.ColumnName);
		//}
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(d, destination.DbSequence.ColumnName);

		sq.Select(rm, relation.RootIdColumn);

		sq.Select(d);

		//Rename the existing ID column and select it as the original ID
		var originIdSelectItem = sq.GetSelectableItems().Where(x => x.Alias.IsEqualNoCase(destination.DbSequence.ColumnName)).First();
		originIdSelectItem.SetAlias(relation.OriginIdColumn);

		//reverse sign
		var columns = sq.GetSelectableItems();
		foreach (var column in columns)
		{
			if (op.ReverseColumns.Contains(column.Alias, StringComparer.OrdinalIgnoreCase))
			{
				var c = (ColumnValue)column.Value;
				c.AddOperatableValue("*", "-1");
			}
		};

		sq.Select(rm, source.GetSequence().ColumnName);

		if (request.SelectQuery.GetColumnNames().Where(x => x.IsEqualNoCase(relation.RemarksColumn)).Any())
		{
			sq.Select(rm, relation.RemarksColumn);
		}
		else
		{
			sq.Select("'force'").As(relation.RemarksColumn);
		}

		return sq;
	}

	private ReverseMaterial ToReverseMaterial(Material material)
	{
		var transaction = material.InterlinkTransaction;

		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var sourceId = source.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkDatasource.InterlinkDatasourceId)).First();

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var relation = transaction.InterlinkDestination.GetInterlinkRelationTable(Environment);

		var destination = transaction.InterlinkDestination;

		return new ReverseMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,
			RootIdColumn = relation.RootIdColumn,
			OriginIdColumn = relation.OriginIdColumn,
			InterlinkRemarksColumn = relation.RemarksColumn,
			DestinationTable = destination.DbTable.TableFullName,
			DestinationColumns = destination.DbTable.ColumnNames,
			DestinationIdColumn = destination.DbSequence.ColumnName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkRelationTable = relation.Definition.TableFullName,
			//DatasourceKeyColumns = source.GetPrimaryKeys().Select(x => x.ColumnName).ToList(),
			//KeyRelationTable = null!,
			//ActionColumn = process.ActionNameColumn,
			InterlinkDatasourceIdColumn = sourceId.ColumnName,
			//InsertCountColumn = process.InsertCountColumn,
			//KeyMapTableNameColumn = process.KeyMapTableNameColumn,
			//KeyRelationTableNameColumn = process.KeyRelationTableNameColumn,
			//ProcessTableName = process.Definition.TableFullName,
			//InterlinkTransactionIdColumn = process.InterlinkTransactionIdColumn,
			//InterlinkDatasourceId = 0!,
			//InterlinkDestinationId = destination.InterlinkDestinationId,
			InterlinkTransaction = transaction,
			Environment = Environment
		};
	}
}

[GeneratePrivateProxy(typeof(ReverseDatasourceMaterializer))]
public partial struct ReverseDatasourceMaterializerProxy;
