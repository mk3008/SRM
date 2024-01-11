using PostgresSample;

using (var connection = PostgresDB.ConnectionOpenAsNew(new ConsoleLogger()))
{
	ApplicationInitializer.CreateSystemTable(connection);
	ApplicationInitializer.CreateApplicationMasterData(connection);
	ApplicationInitializer.CreateApplicationTable(connection);
	ApplicationInitializer.CreateApplicationTransactionData(connection);
}

Console.WriteLine("--AdditionalTransfer ----------------------------------------------------------------------------");

AdditionalTransfer.CreateRequest();
AdditionalTransfer.Execute();

Console.WriteLine("--ValidationTransfer----------------------------------------------------------------------------");

ValidationTransfer.CreateRequest();
ValidationTransfer.Execute();

//Console.WriteLine("--ReverseTransfer----------------------------------------------------------------------------");

//ReverseTransfer.CreateRequest();
//ReverseTransfer.Execute();
