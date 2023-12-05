﻿using Dapper;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
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

		var material = Materializer.Create(connection, datasource, injector);
		if (material == null || material.Count == 0) return;

		// create process row
		var process = CreateProcessRow(datasource, transaction.TransactionId, material.Count);
		process.ProcessId = connection.Execute(Environment.CreateProcessInsertQuery(process));

		material.ExecuteTransfer(connection, process.ProcessId);
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