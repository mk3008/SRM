using Dapper;
using InterlinkMapper.Services;
using PostgresSample;
using RedOrb;

using (var connection = PostgresDB.ConnectionOpenAsNew(new ConsoleLogger()))
{
	ApplicationInitializer.CreateSystemTable(connection);
	ApplicationInitializer.CreateApplicationMasterData(connection);
	ApplicationInitializer.CreateApplicationTable(connection);
	ApplicationInitializer.CreateApplicationTransactionData(connection);
}

AdditionalTransfer.CreateRequest();
AdditionalTransfer.Execute();

ReverseTransfer.CreateRequest();
ReverseTransfer.Execute();
