using InterlinkMapper.Models;
using Microsoft.VisualStudio.TestPlatform.PlatformAbstractions.Interfaces;
using RedOrb;
using System;
using Xunit.Abstractions;

namespace PostgresTest;

public class UnitTest1 : IClassFixture<PostgresDB>
{
	public UnitTest1(PostgresDB postgresDB, ITestOutputHelper output)
	{
		PostgresDB = postgresDB;
		Logger = new UnitTestLogger() { Output = output };

		using var cn = PostgresDB.ConnectionOpenAsNew(Logger);

		var datasource = DatasourceRepository.sales;
		var destination = datasource.Destination;
		Environment = new SystemEnvironment();

		cn.CreateTableOrDefault(Environment.GetInterlinkTansactionTable().Definition);
		cn.CreateTableOrDefault(Environment.GetInterlinkProcessTable().Definition);
		cn.CreateTableOrDefault(Environment.GetInterlinkRelationTable(destination).Definition);
		cn.CreateTableOrDefault(Environment.GetKeyMapTable(datasource).Definition);
		cn.CreateTableOrDefault(Environment.GetKeyRelationTable(datasource).Definition);
	}

	private readonly PostgresDB PostgresDB;

	private readonly UnitTestLogger Logger;

	private readonly SystemEnvironment Environment;

	[Fact]
	public void CreateTransactionInsertQuery()
	{
		//var datasource = DatasourceRepository.sales;
		//var destination = datasource.Destination;
		//var def = Environment.GetInterlinkRelationTable(destination).Definition;
	}
}