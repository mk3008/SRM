﻿using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class ValidationMaterializer
{
	public ValidationMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public string RequestMaterialName { get; set; } = "__validation_request";

	public string DatasourceMaterialName { get; set; } = "__validation_datasource";

	public MaterializeResult? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		if (datasource.Destination.ReverseOption == null) throw new NotSupportedException();

		var requestMaterialQuery = CreateRequestMaterialTableQuery(datasource);

		var requestMaterial = ExecuteMaterialQuery(connection, requestMaterialQuery);

		if (requestMaterial.Count == 0) return null;

		ExecuteDeleteOriginRequest(connection, requestMaterial, datasource);
		var deleteRows = ExecuteCleanUpMaterialRequest(connection, requestMaterial, datasource);

		// If all requests are deleted, there are no processing targets.
		if (requestMaterial.Count == deleteRows) return null;

		var datasourceMaterialQuery = CreateValidationDatasourceMaterialQuery(requestMaterial, datasource, injector);
		return ExecuteMaterialQuery(connection, datasourceMaterialQuery);
	}

	private MaterializeResult ExecuteMaterialQuery(IDbConnection connection, CreateTableQuery createTableQuery)
	{
		var tableName = createTableQuery.TableFullName;

		connection.Execute(createTableQuery, commandTimeout: CommandTimeout);

		var rows = connection.ExecuteScalar<int>(createTableQuery.ToCountQuery());

		return new MaterializeResult
		{
			Count = rows,
			MaterialName = tableName,
			SelectQuery = createTableQuery.ToSelectQuery(),
		};
	}

	private int ExecuteDeleteOriginRequest(IDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CreateOriginDeleteQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private int ExecuteCleanUpMaterialRequest(IDbConnection connection, MaterializeResult result, DbDatasource datasource)
	{
		var query = CleanUpMaterialRequestQuery(result, datasource);
		return connection.Execute(query, commandTimeout: CommandTimeout);
	}

	private CreateTableQuery CreateRequestMaterialTableQuery(DbDatasource datasource)
	{
		var request = Environment.GetValidationRequestTable(datasource);
		return request.ToSelectQuery().ToCreateTableQuery(RequestMaterialName);
	}

	private DeleteQuery CreateOriginDeleteQuery(MaterializeResult result, DbDatasource datasource)
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

	private DeleteQuery CleanUpMaterialRequestQuery(MaterializeResult result, DbDatasource datasource)
	{
		var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("If it does not exist in the keymap table, remove it from the target");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(() =>
		{
			// not exists (select * from RELATION x where d.id = x.id)
			var q = new SelectQuery();
			var (_, x) = q.From(keymap.Definition.TableFullName).As("x");
			q.Where(x, datasource.Destination.Sequence.Column).Equal(r, datasource.Destination.Sequence.Column);
			q.SelectAll();
			return q.ToNotExists();
		});

		sq.Select(r, datasource.Destination.Sequence.Column);

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateExpectValueSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var validation = Environment.GetValidationRequestTable(datasource);
		var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("expected value");
		var (f, d) = sq.From(datasource.Destination.ToSelectQuery()).As("d");
		var m = f.InnerJoin(keymap.Definition.TableFullName).As("m").On(x => new ColumnValue(d, datasource.Destination.Sequence.Column).Equal(x.Table, datasource.Destination.Sequence.Column));

		sq.Select(d);

		keymap.DatasourceKeyColumns.ForEach(x => sq.Select(m, x));

		//exists (select * from REQUEST x where d.id = x.id)
		sq.Where(() =>
		{
			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");

			keymap.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(m, key);
			});
			q.SelectAll();

			return q.ToExists();
		});

		return sq;
	}

	private SelectQuery CreateActualValueSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var validation = Environment.GetValidationRequestTable(datasource);
		var keymap = Environment.GetKeymapTable(datasource);

		var sq = new SelectQuery();
		sq.AddComment("actual value");
		var (f, d) = sq.From(datasource.ToSelectQuery()).As("d");

		sq.Select(d);

		//exists (select * from REQUEST x where d.id = x.id)
		sq.Where(() =>
		{
			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");

			keymap.DatasourceKeyColumns.ForEach(key =>
			{
				q.Where(x, key).Equal(d, key);
			});
			q.SelectAll();

			return q.ToExists();
		});

		return sq;
	}

	private SelectQuery CreateDiffSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var op = datasource.Destination.ReverseOption!;

		var sq = new SelectQuery();
		var expect = sq.With(CreateExpectValueSelectQuery(request, datasource)).As("_expect");
		var actual = sq.With(CreateActualValueSelectQuery(request, datasource)).As("_actual");

		var key = datasource.KeyColumns.First().ColumnName;

		var (f, e) = sq.From(expect).As("e");
		var a = f.LeftJoin(actual).As("a").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(e, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		var validationColumns = e.GetColumnNames()
			.Where(x => !op.ExcludedColumns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.Where(x => datasource.Destination.Table.Columns.Contains(x, StringComparer.OrdinalIgnoreCase))
			.ToList();

		sq.Where(() =>
		{
			//removed
			var condition = new ColumnValue(a, key).IsNull();

			//value changed
			validationColumns.ForEach(column =>
			{
				condition.Or(new ColumnValue(e, column).NotEqual(a, column));
				condition.Or((new ColumnValue(e, column).IsNotNull()).And(new ColumnValue(a, column).IsNull).ToGroup());
				condition.Or((new ColumnValue(e, column).IsNull()).And(new ColumnValue(a, column).IsNotNull).ToGroup());
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
			arg.Add("'{\"changed\":['");

			validationColumns.ForEach(column =>
			{
				var changecase = new CaseExpression();

				var condition = new ColumnValue(e, column).NotEqual(a, column);
				condition.Or((new ColumnValue(e, column).IsNotNull()).And(new ColumnValue(a, column).IsNull).ToGroup());
				condition.Or((new ColumnValue(e, column).IsNull()).And(new ColumnValue(a, column).IsNotNull).ToGroup());

				changecase.When(condition).Then($"'\"{column}\",'");

				arg.Add(changecase);
			});
			arg.Add("']}'");

			var exp = new CaseExpression();
			exp.When(new ColumnValue(a, key).IsNull()).Then("""'{"deleted":true}'""");
			exp.Else(new FunctionValue("concat", arg));

			return exp;
		}).As("remarks");

		return sq;
	}

	private SelectQuery CreateValidationDatasourceSelectQuery(MaterializeResult request, DbDatasource datasource)
	{
		var validation = Environment.GetValidationRequestTable(datasource);
		var relation = Environment.GetRelationTable(datasource.Destination);
		var process = Environment.GetProcessTable();
		var op = datasource.Destination.ReverseOption!;

		var sq = new SelectQuery();
		sq.AddComment("data source to be reverse");
		var (f, d) = sq.From(datasource.Destination.ToSelectQuery()).As("d");
		var r = f.InnerJoin(relation.Definition.TableFullName).As("r").On(x => new ColumnValue(d, datasource.Destination.Sequence.Column).Equal(x.Table, datasource.Destination.Sequence.Column));
		var p = f.InnerJoin(process.Definition.TableFullName).As("p").On(x => new ColumnValue(r, process.ProcessIdColumn).Equal(x.Table, process.ProcessIdColumn));

		sq.Select(d);

		sq.Select(p, process.KeymapTableNameColumn);

		//exists (select * from REQUEST x where d.id = x.id)
		sq.Where(() =>
		{
			var q = new SelectQuery();
			q.AddComment("exists request material");

			var (_, x) = q.From(request.MaterialName).As("x");
			q.Where(x, datasource.Destination.Sequence.Column).Equal(d, datasource.Destination.Sequence.Column);
			q.SelectAll();

			return q.ToExists();
		});

		return sq;
	}

	private CreateTableQuery CreateValidationDatasourceMaterialQuery(MaterializeResult request, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateValidationDatasourceSelectQuery(request, datasource)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");

		sq.Select(datasource.Destination.Sequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery(DatasourceMaterialName);
	}
}

[GeneratePrivateProxy(typeof(ValidationMaterializer))]
public partial struct ValidationMaterializerProxy;