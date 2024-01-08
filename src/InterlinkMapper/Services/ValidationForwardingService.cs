using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using PrivateProxy;
using RedOrb;
using System.Data;

namespace InterlinkMapper.Services;

public class ValidationForwardingService
{
	public ValidationForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new ValidationMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private ValidationMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, InterlinkDatasource datasource)
	{
		Execute(connection, datasource, null);
	}

	public void Execute(IDbConnection connection, InterlinkDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		var destination = datasource.Destination;

		var transaction = CreateTransactionAsNew(destination);
		connection.Save(transaction);

		var material = Materializer.Create(connection, transaction, datasource, injector);
		if (material == null) return;

		//transfer
		ExecuteReverse(connection, transaction, datasource, material);
		ExecuteAdditional(connection, transaction, datasource, material);
	}

	private void ExecuteReverse(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, ValidationMaterial validation)
	{
		var destination = transaction.InterlinkDestination;

		var source = ObjectRelationMapper.FindFirst<InterlinkDatasource>();
		var sourceId = source.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkDatasource.InterlinkDatasourceId)).First();

		var proc = ObjectRelationMapper.FindFirst<InterlinkProcess>();
		var procId = proc.ColumnDefinitions.Where(x => x.Identifer == nameof(InterlinkProcess.InterlinkProcessId)).First();

		var relation = transaction.InterlinkDestination.GetInterlinkRelationTable(Environment);

		var m = new DatasourceReverseMaterial
		{
			CommandTimeout = CommandTimeout,
			DestinationColumns = destination.DbTable.ColumnNames,
			DestinationSeqColumn = destination.DbSequence.ColumnName,
			DestinationTable = destination.TableFullName,
			Environment = Environment,
			InterlinkDatasource = datasource,
			InterlinkDatasourceIdColumn = sourceId.ColumnName,
			InterlinkProcessIdColumn = procId.ColumnName,
			InterlinkRelationTable = relation.Definition.TableFullName,
			InterlinkRemarksColumn = relation.RemarksColumn,
			InterlinkTransaction = transaction,
			MaterialName = validation.MaterialName,
			OriginIdColumn = relation.OriginIdColumn,
			PlaceHolderIdentifer = Environment.DbEnvironment.PlaceHolderIdentifer,
			RootIdColumn = relation.RootIdColumn,
			SelectQuery = validation.SelectQuery,
		};

		m.ExecuteTransfer(connection);
	}

	private void ExecuteAdditional(IDbConnection connection, InterlinkTransaction transaction, InterlinkDatasource datasource, ValidationMaterial validation)
	{
		var request = validation.ToAdditionalRequestMaterial();

		var materializer = new AdditionalDatasourceMaterializer(Environment)
		{
			MaterialName = "__validation_additional_datasource"
		};
		var material = materializer.Create(connection, transaction, datasource, request);
		material.ExecuteTransfer(connection);
	}

	private InterlinkTransaction CreateTransactionAsNew(InterlinkDestination destination, string argument = "")
	{
		var tran = new InterlinkTransaction()
		{
			InterlinkDestination = destination,
			ServiceName = nameof(ValidationForwardingService),
			Argument = argument
		};
		return tran;
	}
}

[GeneratePrivateProxy(typeof(ValidationForwardingService))]
public partial struct ValidationForwardingServiceProxy;