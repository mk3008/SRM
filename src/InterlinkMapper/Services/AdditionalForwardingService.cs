﻿using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using PrivateProxy;
using System.Data;

namespace InterlinkMapper.Services;

public class AdditionalForwardingService
{
	public AdditionalForwardingService(SystemEnvironment environment)
	{
		Environment = environment;
		Materializer = new AdditionalForwardingMaterializer(Environment);
	}

	private SystemEnvironment Environment { get; init; }

	private AdditionalForwardingMaterializer Materializer { get; init; }

	public int CommandTimeout => Environment.DbEnvironment.CommandTimeout;

	public void Execute(IDbConnection connection, DbDatasource datasource, Func<SelectQuery, SelectQuery>? injector)
	{
		// create transaction row
		var transaction = CreateTransactionRow(datasource);
		transaction.TransactionId = connection.Execute(Environment.CreateTransactionInsertQuery(transaction));

		var datasourceMaterial = Materializer.Create(connection, datasource, injector);
		if (datasourceMaterial == null || datasourceMaterial.Count == 0) return;

		// create process row
		var process = CreateProcessRow(datasource, transaction.TransactionId, datasourceMaterial.Count);
		process.ProcessId = connection.Execute(Environment.CreateProcessInsertQuery(process));

		// transfer datasource
		connection.Execute(datasource.Destination.CreateInsertQueryFrom(datasourceMaterial), commandTimeout: CommandTimeout);

		// create system relation mapping
		connection.Execute(Environment.CreateKeymapInsertQuery(datasource, datasourceMaterial), commandTimeout: CommandTimeout);
		connection.Execute(Environment.CreateRelationInsertQuery(datasource, datasourceMaterial, process.ProcessId), commandTimeout: CommandTimeout);
	}

	private TransactionRow CreateTransactionRow(DbDatasource datasource, string argument = "")
	{
		var row = new TransactionRow()
		{
			DestinationId = datasource.Destination.DestinationId,
			DatasourceId = datasource.DatasourceId,
			Argument = argument
		};
		return row;
	}

	private ProcessRow CreateProcessRow(DbDatasource datasource, long transactionId, int insertCount)
	{
		var keymap = Environment.GetKeymapTable(datasource);
		var row = new ProcessRow()
		{
			ActionName = nameof(AdditionalForwardingService),
			TransactionId = transactionId,
			InsertCount = insertCount,
			KeymapTableName = keymap.Definition.TableFullName,
		};
		return row;
	}
}

[GeneratePrivateProxy(typeof(AdditionalForwardingService))]
public partial struct AdditionalForwardingServiceProxy;