using Carbunql.Tables;
using InterlinkMapper.Models;
using InterlinkMapper.Services;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ValidationMaterializer : IRequestMaterializer
{
	public ValidationMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RequestMaterialName { get; set; } = "__validation_request";

	public string DatasourceMaterialName { get; set; } = "__validation_datasource";

	public string RowNumberColumnName { get; set; } = "row_num";

	private string ExprectCteName = "expect_data";

	private string ActualCteName = "actual_data";

	private string DiffCteName = "diff_data";

	public ValidationMaterial? Create(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var destination = transaction.InterlinkDestination;

		if (!destination.AllowReverse) throw new NotSupportedException();
		if (string.IsNullOrEmpty(Environment.DbEnvironment.LengthFunction)) throw new NullReferenceException(nameof(Environment.DbEnvironment.LengthFunction));

		var requestMaterialQuery = CreateRequestMaterialQuery(datasource);
		var request = this.CreateMaterial(connection, transaction, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, datasource, request);

		var query = CreateValidationMaterialQuery(datasource, request, injector);
		var validation = this.CreateMaterial(connection, transaction, query);

		return ToValidationMaterial(datasource, validation);
	}

	private ValidationMaterial ToValidationMaterial(InterlinkDatasource datasource, Material material)
	{
		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var sourceId = source.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkDatasource.InterlinkDatasourceId)).First();

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);
		var keymap = datasource.GetKeyMapTable(Environment);
		var keyrelation = datasource.GetKeyRelationTable(Environment);

		return new ValidationMaterial
		{
			//Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,

			DatasourceKeyColumns = datasource.KeyColumns.Select(x => x.ColumnName).ToList(),
			RootIdColumn = relation.RootIdColumn,
			OriginIdColumn = relation.OriginIdColumn,
			InterlinkRemarksColumn = relation.RemarksColumn,
			DestinationTable = datasource.Destination.DbTable.TableFullName,
			DestinationColumns = datasource.Destination.DbTable.ColumnNames,
			DestinationIdColumn = datasource.Destination.DbSequence.ColumnName,
			KeyMapTableFullName = keymap.Definition.TableFullName,
			KeyRelationTableFullName = keyrelation.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkRelationTable = relation.Definition.TableFullName,

			//KeymapTableNameColumn = process.KeyMapTableNameColumn,
			//InterlinkTransactionIdColumn = process.InterlinkTransactionIdColumn,

			//ActionColumn = process.ActionNameColumn,
			InterlinkDatasourceIdColumn = sourceId.ColumnName,
			//InsertCountColumn = process.InsertCountColumn,
			//KeyMapTableNameColumn = process.KeyMapTableNameColumn,
			//KeyRelationTableNameColumn = process.KeyRelationTableNameColumn,
			//ProcessTableName = process.Definition.TableFullName,

			//InterlinkDatasourceId = datasource.InterlinkDatasourceId,
			//InterlinkDestinationId = datasource.Destination.InterlinkDestinationId,
			InterlinkTransaction = material.InterlinkTransaction,
			Environment = Environment,
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, InterlinkDatasource datasource, Material result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(InterlinkDatasource datasource)
	{
		var request = datasource.GetValidationRequestTable(Environment);
		var keymap = datasource.GetKeyMapTable(Environment);

		var sq = new SelectQuery();
		var (f, r) = sq.From(request.Definition.TableFullName).As("r");
		var m = f.InnerJoin(keymap.Definition.TableFullName).As("m").On(r, datasource.KeyColumns.Select(x => x.ColumnName));

		sq.Select(r, request.RequestIdColumn);
		sq.Select(m, datasource.Destination.DbSequence.ColumnName);
		datasource.KeyColumns.ForEach(key => sq.Select(m, key.ColumnName));

		// If the destination sequence value is NULL,
		// it means the transfer is stopped.
		// Reverse processing is also not required.
		sq.Where(m, datasource.Destination.DbSequence.ColumnName).IsNotNull();

		return sq.ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(InterlinkDatasource datasource, Material result)
	{
		var request = datasource.GetValidationRequestTable(Environment);
		var requestTable = request.Definition.TableFullName;
		var requestId = request.RequestIdColumn;

		var sq = new SelectQuery();
		sq.AddComment("data that has been materialized will be deleted from the original.");

		var (_, r) = sq.From(result.SelectQuery).As("r");

		sq.Select(r, requestId);

		return sq.ToDeleteQuery(requestTable);
	}

	private SelectQuery CreateExpectValueSelectQuery(InterlinkDatasource datasource, Material request)
	{
		//var validation = Environment.GetValidationRequestTable(datasource);
		//var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("inject request material filter");

		var (f, d) = sq.From(datasource.Destination.ToSelectQuery()).As("d");
		f.InnerJoin(request.MaterialName).As("rm").On(d, datasource.Destination.DbSequence.ColumnName);
		sq.Select(d);

		return sq;
	}

	private SelectQuery CreateActualValueSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var validation = datasource.GetValidationRequestTable(Environment);
		var keymap = datasource.GetKeyMapTable(Environment);

		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			InjectRequestFilter(cte, datasource, request);
		}

		var sq = new SelectQuery();

		var (f, d) = sq.From(ds).As("d");

		sq = InjectRequestFilter(sq, datasource, request);
		sq.AddComment("does not exist if physically deleted");

		sq.Select(d, overwrite: false);

		return sq;
	}

	private SelectQuery InjectRequestFilter(SelectQuery sq, InterlinkDatasource datasource, Material request)
	{
		sq.AddComment("inject request material filter");

		var keymap = datasource.GetKeyMapTable(Environment);
		var f = sq.FromClause!;
		var d = f.Root;
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(d, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		sq.Select(rm, datasource.Destination.DbSequence.ColumnName);

		return sq;
	}

	private SelectQuery CreateDeletedDiffSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);

		var op = datasource.Destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("reverse only");

		var expect = sq.With(CreateExpectValueSelectQuery(datasource, request)).As(ExprectCteName);
		var actual = sq.With(CreateActualValueSelectQuery(datasource, request)).As(ActualCteName);

		var key = datasource.KeyColumns.First().ColumnName;

		var (f, e) = sq.From(expect).As("e");
		var a = f.LeftJoin(actual).As("a").On(e, datasource.Destination.DbSequence.ColumnName);

		var validationColumns = e.GetColumnNames()
			.Where(x => !op.ExcludedColumns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.Where(x => datasource.Destination.DbTable.ColumnNames.Contains(x, StringComparer.OrdinalIgnoreCase))
			.ToList();

		sq.Where(a, key).IsNull();

		sq.Select(e, datasource.Destination.DbSequence.ColumnName);
		datasource.KeyColumns.ForEach(key => sq.Select(a, key.ColumnName));

		sq.Select("'{\"deleted\":true}'").As(relation.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateUpdatedDiffSubQuery(InterlinkDatasource datasource, Material request)
	{
		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);
		var op = datasource.Destination.ReverseOption;

		var sq = new SelectQuery();
		var expect = sq.With(CreateExpectValueSelectQuery(datasource, request)).As(ExprectCteName);
		var actual = sq.With(CreateActualValueSelectQuery(datasource, request)).As(ActualCteName);

		var key = datasource.KeyColumns.First().ColumnName;

		var (f, e) = sq.From(expect).As("e");
		var a = f.InnerJoin(actual).As("a").On(e, datasource.Destination.DbSequence.ColumnName);

		var validationColumns = e.GetColumnNames()
			.Where(x => !op.ExcludedColumns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.Where(x => datasource.Destination.DbTable.ColumnNames.Contains(x, StringComparer.OrdinalIgnoreCase))
			.ToList();

		sq.Where(() =>
		{
			//removed
			var condition = new LiteralValue("false");

			//value changed
			validationColumns.ForEach(column =>
			{
				condition.Or(CreateNullSafeEqualityValue(e, column, a, column));
			});

			return condition;
		});

		sq.Select(e, datasource.Destination.DbSequence.ColumnName);
		datasource.KeyColumns.ForEach(key => sq.Select(a, key.ColumnName));

		//diff info
		sq.Select(() =>
		{
			//value changed			
			var arg = new ValueCollection();

			validationColumns.ForEach(column =>
			{
				var changecase = new CaseExpression();
				var condition = CreateNullSafeEqualityValue(e, column, a, column);
				changecase.When(condition).Then($"'\"{column}\",'");

				arg.Add(changecase);
			});

			return new FunctionValue("concat", arg);
		}).As(relation.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateUpdatedDiffSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var relation = datasource.Destination.GetInterlinkRelationTable(Environment);

		var sq = new SelectQuery();
		sq.AddComment("reverse and additional");
		var (f, d) = sq.From(CreateUpdatedDiffSubQuery(datasource, request)).As("d");

		sq.Select(d);
		var remarks = sq.GetSelectableItems().Where(x => x.Alias == relation.RemarksColumn).First();
		sq.SelectClause!.Remove(remarks);

		var length_arg = new ValueCollection
		{
			new ColumnValue(d, relation.RemarksColumn)
		};

		var length_value = new FunctionValue(Environment.DbEnvironment.LengthFunction, length_arg);
		length_value.AddOperatableValue("-", "1");

		var substring_arg = new ValueCollection
		{
			new ColumnValue(d, relation.RemarksColumn),
			"1",
			length_value
		};

		var concat_arg = new ValueCollection
		{
			"'{\"updated\":['",
			new FunctionValue("substring", substring_arg),
			"']}'"
		};

		sq.Select(new FunctionValue("concat", concat_arg)).As(relation.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateValidationDatasourceSelectQuery(InterlinkDatasource datasource, Material request)
	{
		var sq = CreateDeletedDiffSelectQuery(datasource, request);

		sq.UnionAll(CreateUpdatedDiffSelectQuery(datasource, request));

		return sq;
	}

	private CreateTableQuery CreateValidationMaterialQuery(InterlinkDatasource datasource, Material request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var diff = sq.With(CreateValidationDatasourceSelectQuery(datasource, request)).As(DiffCteName);

		var (_, d) = sq.From(diff).As("d");

		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}

	private ValueBase CreateNullSafeEqualityValue(SelectableTable leftTable, string leftColumn, SelectableTable rightTable, string rightColumn)
	{
		var op = Environment.DbEnvironment.NullSafeEqualityOperator;
		if (string.IsNullOrEmpty(op))
		{
			var condition = new ColumnValue(leftTable, leftColumn).NotEqual(rightTable, rightColumn);
			condition.Or((new ColumnValue(leftTable, leftColumn).IsNotNull()).And(new ColumnValue(rightTable, rightColumn).IsNull).ToGroup());
			condition.Or((new ColumnValue(leftTable, leftColumn).IsNull()).And(new ColumnValue(rightTable, rightColumn).IsNotNull).ToGroup());
			return condition;
		}
		else
		{
			return ValueParser.Parse($"{leftTable.Alias}.{leftColumn} {op} {rightTable.Alias}.{rightColumn}");
		}
	}
}

[GeneratePrivateProxy(typeof(ValidationMaterializer))]
public partial struct ValidationMaterializerProxy;