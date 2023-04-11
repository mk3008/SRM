using Carbunql;
using Carbunql.Building;
using Carbunql.Dapper;
using InterlinkMapper.Data;
using System.Data;

namespace InterlinkMapper;

public class Synchronizer
{
	public Synchronizer(Database database)
	{
		Database = database;
	}

	public Database Database { get; init; }

	public Func<SelectQuery, string, SelectQuery>? Injector { get; set; }

	private string GenerateBrigeName(Datasource datasource)
	{
		return "_" + datasource.DatasourceName.ToHash();
	}

	public void InsertIfNotExists(IDbConnection cn, Datasource datasource, string arguments)
	{
		var bridgename = GenerateBrigeName(datasource);

		var trn = Database.RegistAndGetBatchTransactionAsNew(cn, datasource, arguments);
		var proc = Database.RegistAndGetBatchProcessAsNew(cn, trn, datasource);

		var createq = datasource.BuildCreateBridgeTableQuery(bridgename, (x) =>
		{
			if (Injector == null) return x;
			return Injector(x, arguments);
		});
		cn.Execute(createq);

		var bridgeq = datasource.BuildSelectBridgeQuery(bridgename);

		var cnt = cn.ExecuteScalar<int>(bridgeq.ToCountQuery());
		if (cnt == 0) return;

		Database.InsertDestination(cn, proc, bridgeq);
		Database.InsertProcessMap(cn, proc, bridgeq);
		Database.InsertKeyMap(cn, proc, bridgeq);
		Database.InsertRelationMap(cn, proc, bridgeq);
		Database.InsertHoldMap(cn, proc, bridgeq);
	}

	public void DeleteIfExists(IDbConnection cn, Datasource datasource, string arguments)
	{
		var bridgename = GenerateBrigeName(datasource);

		var trn = Database.RegistAndGetBatchTransactionAsNew(cn, datasource, arguments);
		var proc = Database.RegistAndGetBatchProcessAsNew(cn, trn, datasource);

		var createq = datasource.BuildCreateBridgeTableQuery(bridgename, (x) =>
		{
			if (Injector == null) return x;
			return Injector(x, arguments);
		});
		cn.Execute(createq);

		var bridgeq = datasource.BuildSelectBridgeQuery(bridgename);

		var cnt = cn.ExecuteScalar<int>(bridgeq.ToCountQuery());
		if (cnt == 0) return;

		Database.InsertDestination(cn, proc, bridgeq);
		Database.DeleteProcessMap(cn, proc, bridgeq);
		Database.DeleteKeyMap(cn, proc, bridgeq);
		Database.DeleteRelationMap(cn, proc, bridgeq);
	}

	public void ReverseIfDifference(IDbConnection cn, BatchTransaction validateTransaction, string arguments)
	{
		var datasource = validateTransaction.Datasource;
		if (datasource.Destination == null) throw new Exception();
		var bridgename = GenerateBrigeName(datasource);

		var trn = Database.RegistAndGetBatchTransactionAsNew(cn, datasource, arguments);
		var proc = Database.RegistAndGetBatchProcessAsNew(cn, trn, datasource);

		var procTable = Database.ProcessTableName;
		var procMapTable = Database.ProcessMapNameBuilder(datasource.Destination);
		var keyMapTable = Database.KeyMapNameBuilder(datasource);
		var procIdColumnName = Database.ProcessIdColumnName;
		var tranIdColumnName = Database.TransctionIdColumnName;
		var palceholder = Database.PlaceholderIdentifier;

		var createq = datasource.GenerateSelectDatasourceQueryIfDifference(trn.TransactionId, procMapTable, procTable, keyMapTable, procIdColumnName, tranIdColumnName, palceholder);
		cn.Execute(createq);

		var bridgeq = datasource.BuildSelectBridgeQuery(bridgename);

		var cnt = cn.ExecuteScalar<int>(bridgeq.ToCountQuery());
		if (cnt == 0) return;

		Database.InsertDestination(cn, proc, bridgeq);
		Database.InsertProcessMap(cn, proc, bridgeq);
		Database.DeleteKeyMap(cn, proc, bridgeq);
		Database.InsertRelationMap(cn, proc, bridgeq);
	}

	public void Upsert(IDbConnection cn, Datasource datasource, string arguments)
	{
		var bridgename = GenerateBrigeName(datasource);

		var trn = Database.RegistAndGetBatchTransactionAsNew(cn, datasource, arguments);
		var proc = Database.RegistAndGetBatchProcessAsNew(cn, trn, datasource);

		var createq = datasource.BuildCreateBridgeTableQuery(bridgename, (x) =>
		{
			if (Injector == null) return x;
			return Injector(x, arguments);
		});
		cn.Execute(createq);

		var bridgeq = datasource.BuildSelectBridgeQuery(bridgename);

		var cnt = cn.ExecuteScalar<int>(bridgeq.ToCountQuery());
		if (cnt == 0) return;

		Database.InsertDestination(cn, proc, bridgeq);
		Database.InsertProcessMap(cn, proc, bridgeq);
		Database.InsertKeyMapIfNotExists(cn, proc, bridgeq);
		Database.InsertRelationMap(cn, proc, bridgeq);
	}
}
