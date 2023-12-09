﻿using Carbunql.Tables;
using InterlinkMapper;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Materializer;

public class AdditionalForwardingMaterializer : IMaterializer
{
	public AdditionalForwardingMaterializer(SystemEnvironment environment)
	{
		Environment = environment;
	}

	public string RowNumberColumnName { get; set; } = "row_num";

	private SystemEnvironment Environment { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public AdditionalMaterial? Create(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var requestMaterialQuery = CreateRequestMaterialQuery(datasource);
		var request = this.CreateMaterial(connection, requestMaterialQuery);
		if (request.Count == 0) return null;

		DeleteOriginRequest(connection, datasource, request);
		var deleteRows = CleanUpMaterialRequest(connection, datasource, request);

		// If all requests are deleted, there are no processing targets.
		if (request.Count == deleteRows) return null;

		var query = CreateAdditionalMaterialQuery(datasource, request, injector);
		var additional = this.CreateMaterial(connection, query);

		return ToAdditionalMaterial(datasource, additional);
	}

	private AdditionalMaterial ToAdditionalMaterial(DbDatasource datasource, Material material)
	{
		var process = Environment.GetProcessTable();
		var relation = Environment.GetRelationTable(datasource.Destination);
		var keymap = Environment.GetKeyMapTable(datasource);
		var history = Environment.GetKeyRelationTable(datasource);
		var reverse = Environment.GetReverseTable(datasource.Destination);

		return new AdditionalMaterial
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
			KeyMapTable = keymap.Definition.TableFullName,
			KeyRelationTable = history.Definition.TableFullName,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			CommandTimeout = Environment.DbEnvironment.CommandTimeout,
			ProcessIdColumn = process.ProcessIdColumn,
			RelationTable = relation.Definition.TableFullName,
			ReverseTable = reverse.Definition.TableFullName,
			NumericType = Environment.DbEnvironment.NumericTypeName,
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
		var request = Environment.GetInsertRequestTable(datasource);
		var keyhistory = Environment.GetKeyRelationTable(datasource);

		var sq = new SelectQuery();
		var (f, r) = sq.From(request.Definition.TableFullName).As("r");

		sq.Select(r, request.RequestIdColumn);

		var args = new ValueCollection();
		datasource.KeyColumns.ForEach(key =>
		{
			args.Add(new ColumnValue(r, key.ColumnName));
			sq.Select(r, key.ColumnName);
		});

		//sq.Select($"cast(null as {Environment.DbEnvironment.TextTypeName})").As(keyhistory.RemarksColumn);

		sq.Select(new FunctionValue("row_number", () =>
		{
			var over = new OverClause();
			over.AddPartition(args);
			over.AddOrder(new SortableItem(new ColumnValue(r, request.RequestIdColumn)));
			return over;
		})).As(RowNumberColumnName);

		var name = "__additional_request";
		return sq.ToCreateTableQuery(name);
	}

	private DeleteQuery CreateOriginDeleteQuery(DbDatasource datasource, Material result)
	{
		var request = Environment.GetInsertRequestTable(datasource);
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
		var keyamp = Environment.GetKeyMapTable(datasource);
		var keymapTable = keyamp.Definition.TableFullName;
		var datasourceKeys = keyamp.DatasourceKeyColumns;

		var sq = new SelectQuery();
		sq.AddComment("exclude requests that exist in the keymap from forwarding");

		var (f, r) = sq.From(result.MaterialName).As("r");

		sq.Where(() =>
		{
			// exists (select * from KEYMAP x where d.key = x.key)
			var q = new SelectQuery();
			var (_, x) = q.From(keymapTable).As("x");
			datasourceKeys.ForEach(key =>
			{
				q.Where(x, key).Equal(r, key);
			});
			q.SelectAll();
			return q.ToExists().Or(r, RowNumberColumnName).NotEqual("1");
		});

		datasourceKeys.ForEach(key => sq.Select(r, key));

		return sq.ToDeleteQuery(result.MaterialName);
	}

	private SelectQuery CreateAdditionalDatasourceSelectQuery(DbDatasource datasource, Material request)
	{
		var ds = datasource.ToSelectQuery();
		var raw = ds.GetCommonTables().Where(x => x.Alias == "__raw").FirstOrDefault();

		if (raw != null && raw.Table is VirtualTable vt && vt.Query is SelectQuery cte)
		{
			InjectRequestMaterialFilter(cte, datasource, request);
		}

		var sq = new SelectQuery();
		var (f, d) = sq.From(ds).As("d");
		sq.Select(d);
		sq = InjectRequestMaterialFilter(sq, datasource, request);

		return sq;
	}

	private SelectQuery InjectRequestMaterialFilter(SelectQuery sq, DbDatasource datasource, Material request)
	{
		sq.AddComment("inject request material filter");

		var reverse = Environment.GetReverseTable(datasource.Destination);
		var f = sq.FromClause!;
		var d = f.Root;

		var rm = f.InnerJoin(request.MaterialName).As("rm").On(x =>
		{
			datasource.KeyColumns.ForEach(key =>
			{
				x.Condition(d, key.ColumnName).Equal(x.Table, key.ColumnName);
			});
		});

		//sq.Select(rm, reverse.RemarksColumn);

		return sq;
	}

	private CreateTableQuery CreateAdditionalMaterialQuery(DbDatasource datasource, Material request, Func<SelectQuery, SelectQuery>? injector)
	{
		var sq = new SelectQuery();
		var _datasource = sq.With(CreateAdditionalDatasourceSelectQuery(datasource, request)).As("_target_datasource");

		var (f, d) = sq.From(_datasource).As("d");
		sq.Select(datasource.Destination.Sequence);
		sq.Select(d);

		if (injector != null)
		{
			sq = injector(sq);
		}

		return sq.ToCreateTableQuery("__datasource");
	}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingMaterializer))]
public partial struct MaterializeServiceProxy;