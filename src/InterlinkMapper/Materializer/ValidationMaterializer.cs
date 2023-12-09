using Carbunql.Tables;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ValidationMaterializer : IMaterializer
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

	public ValidationMaterial? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		if (!datasource.Destination.AllowReverse) throw new NotSupportedException();
		if (string.IsNullOrEmpty(Environment.DbEnvironment.LengthFunction)) throw new NullReferenceException(nameof(Environment.DbEnvironment.LengthFunction));

		var requestMaterialQuery = CreateRequestMaterialQuery(datasource);
		var request = this.CreateMaterial(connection, requestMaterialQuery);

		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, datasource, request);
		var deleteRows = CleanUpMaterialRequest(connection, datasource, request);

		// If all requests are deleted, there are no processing targets.
		if (request.Count == deleteRows) return null;

		var query = CreateValidationMaterialQuery(datasource, request, injector);
		var validation = this.CreateMaterial(connection, query);

		return ToValidationMaterial(datasource, validation);
	}

	private ValidationMaterial ToValidationMaterial(DbDatasource datasource, Material material)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(datasource.Destination);
		var keymap = Environment.GetKeyMapTable(datasource);
		var history = Environment.GetKeyRelationTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		return new ValidationMaterial
		{
			Count = material.Count,
			MaterialName = material.MaterialName,
			SelectQuery = material.SelectQuery,

			DatasourceKeyColumns = datasource.KeyColumns.Select(x => x.ColumnName).ToList(),
			RootIdColumn = reverse.RootIdColumn,
			OriginIdColumn = reverse.OriginIdColumn,
			RemarksColumn = reverse.RemarksColumn,
			DestinationTable = datasource.Destination.Table.GetTableFullName(),
			DestinationColumns = datasource.Destination.Table.Columns,
			DestinationIdColumn = datasource.Destination.Sequence.Column,
			KeymapTable = keymap.Definition.TableFullName,
			KeyRelationTable = history.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			ProcessIdColumn = process.ProcessIdColumn,
			RelationTable = relation.Definition.TableFullName,
			ReverseTable = reverse.Definition.TableFullName,

			KeymapTableNameColumn = process.KeyMapTableNameColumn,
		};
	}

	private int DeleteOriginRequest(IDbConnection connection, DbDatasource datasource, Material result)
	{
		var query = CreateOriginDeleteQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int CleanUpMaterialRequest(IDbConnection connection, DbDatasource datasource, Material result)
	{
		var query = CleanUpMaterialRequestQuery(datasource, result);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialQuery(DbDatasource datasource)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		var keymap = Environment.GetKeyMapTable(datasource);

		var sq = new SelectQuery();
		var (f, r) = sq.From(request.Definition.TableFullName).As("r");
		var m = f.InnerJoin(keymap.Definition.TableFullName).As("m").On(r, datasource.KeyColumns.Select(x => x.ColumnName));

		sq.Select(r, request.RequestIdColumn);
		sq.Select(m, datasource.Destination.Sequence.Column);
		datasource.KeyColumns.ForEach(key => sq.Select(m, key.ColumnName));

		//row_number() over(order by m.sale_journal_id) as row_num
		sq.Select(new FunctionValue("row_number", () =>
		{
			var over = new OverClause();
			over.AddPartition(new ColumnValue(m, datasource.Destination.Sequence.Column));
			over.AddOrder(new SortableItem(new ColumnValue(r, request.RequestIdColumn)));
			return over;
		})).As(RowNumberColumnName);

		return sq.ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(DbDatasource datasource, Material result)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		var requestTable = request.Definition.TableFullName;
		var requestId = request.RequestIdColumn;

		var sq = new SelectQuery();
		sq.AddComment("data that has been materialized will be deleted from the original.");

		var (f, r) = sq.From(requestTable).As("r");

		sq.Where(() =>
		{
			// exists (select * from REQUEST x where d.key = x.key)
			var q = new SelectQuery();
			var (_, x) = q.From(result.MaterialName).As("x");
			q.Where(x, requestId).Equal(r, requestId);
			q.SelectAll();
			return q.ToExists();
		});

		sq.Select(r, requestId);

		return sq.ToDeleteQuery(requestTable);
	}

	private DeleteQuery CleanUpMaterialRequestQuery(DbDatasource datasource, Material result)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		var keymap = Environment.GetKeyMapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("Delete duplicate rows so that the destination ID is unique");

		var (f, rm) = sq.From(result.MaterialName).As("rm");

		sq.Where(rm, RowNumberColumnName).NotEqual("1");

		sq.Select(rm, request.RequestIdColumn);

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateExpectValueSelectQuery(DbDatasource datasource, Material request)
	{
		//var validation = Environment.GetValidationRequestTable(datasource);
		//var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("inject request material filter");

		var (f, d) = sq.From(datasource.Destination.ToSelectQuery()).As("d");
		f.InnerJoin(request.MaterialName).As("rm").On(d, datasource.Destination.Sequence.Column);
		sq.Select(d);

		return sq;
	}

	private SelectQuery CreateActualValueSelectQuery(DbDatasource datasource, Material request)
	{
		var validation = Environment.GetValidationRequestTable(datasource);
		var keymap = Environment.GetKeyMapTable(datasource);

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

	private SelectQuery InjectRequestFilter(SelectQuery sq, DbDatasource datasource, Material request)
	{
		sq.AddComment("inject request material filter");

		var keymap = Environment.GetKeyMapTable(datasource);
		var f = sq.FromClause!;
		var d = f.Root;
		var rm = f.InnerJoin(request.MaterialName).As("rm").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(d, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		sq.Select(rm, datasource.Destination.Sequence.Column);

		return sq;
	}

	private SelectQuery CreateDeletedDiffSelectQuery(DbDatasource datasource, Material request)
	{
		var reverse = Environment.GetReverseTable(datasource.Destination);

		var op = datasource.Destination.ReverseOption;

		var sq = new SelectQuery();
		sq.AddComment("reverse only");

		var expect = sq.With(CreateExpectValueSelectQuery(datasource, request)).As("_expect");
		var actual = sq.With(CreateActualValueSelectQuery(datasource, request)).As("_actual");

		var key = datasource.KeyColumns.First().ColumnName;

		var (f, e) = sq.From(expect).As("e");
		var a = f.LeftJoin(actual).As("a").On(e, datasource.Destination.Sequence.Column);

		var validationColumns = e.GetColumnNames()
			.Where(x => !op.ExcludedColumns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.Where(x => datasource.Destination.Table.Columns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.ToList();

		sq.Where(a, key).IsNull();

		sq.Select(e, datasource.Destination.Sequence.Column);
		datasource.KeyColumns.ForEach(key => sq.Select(a, key.ColumnName));

		sq.Select("'{\"deleted\":true}'").As(reverse.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateUpdatedDiffSubQuery(DbDatasource datasource, Material request)
	{
		var reverse = Environment.GetReverseTable(datasource.Destination);
		var op = datasource.Destination.ReverseOption;

		var sq = new SelectQuery();
		var expect = sq.With(CreateExpectValueSelectQuery(datasource, request)).As("_expect");
		var actual = sq.With(CreateActualValueSelectQuery(datasource, request)).As("_actual");

		var key = datasource.KeyColumns.First().ColumnName;

		var (f, e) = sq.From(expect).As("e");
		var a = f.InnerJoin(actual).As("a").On(e, datasource.Destination.Sequence.Column);

		var validationColumns = e.GetColumnNames()
			.Where(x => !op.ExcludedColumns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.Where(x => datasource.Destination.Table.Columns.Contains(x, StringComparer.OrdinalIgnoreCase))
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

		sq.Select(e, datasource.Destination.Sequence.Column);
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
		}).As(reverse.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateUpdatedDiffSelectQuery(DbDatasource datasource, Material request)
	{
		var reverse = Environment.GetReverseTable(datasource.Destination);

		var sq = new SelectQuery();
		sq.AddComment("reverse and additional");
		var (f, d) = sq.From(CreateUpdatedDiffSubQuery(datasource, request)).As("d");

		sq.Select(d);
		var remarks = sq.GetSelectableItems().Where(x => x.Alias == reverse.RemarksColumn).First();
		sq.SelectClause!.Remove(remarks);

		var length_arg = new ValueCollection
		{
			new ColumnValue(d, reverse.RemarksColumn)
		};

		var length_value = new FunctionValue(Environment.DbEnvironment.LengthFunction, length_arg);
		length_value.AddOperatableValue("-", "1");

		var substring_arg = new ValueCollection
		{
			new ColumnValue(d, reverse.RemarksColumn),
			"1",
			length_value
		};

		var concat_arg = new ValueCollection
		{
			"'{\"updated\":['",
			new FunctionValue("substring", substring_arg),
			"']}'"
		};

		sq.Select(new FunctionValue("concat", concat_arg)).As(reverse.RemarksColumn);

		return sq;
	}

	private SelectQuery CreateValidationDatasourceSelectQuery(DbDatasource datasource, Material request)
	{
		var sq = CreateDeletedDiffSelectQuery(datasource, request);

		sq.UnionAll(CreateUpdatedDiffSelectQuery(datasource, request));

		return sq;
	}

	private CreateTableQuery CreateValidationMaterialQuery(DbDatasource datasource, Material request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateValidationDatasourceSelectQuery(datasource, request)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");

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