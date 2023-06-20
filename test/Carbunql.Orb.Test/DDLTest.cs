using Carbunql.Orb.Test.DBTestModels;

namespace Carbunql.Orb.Test;

public class DDLTest
{
	[Fact]
	public void CreateTable()
	{
		var t = DefinitionRepository.GetTransactionTableDefinition();

		var sql = t.ToCreateTableCommandText();

		Assert.Equal(@"create table if not exists public.transactions (
    transaction_id serial8 not null, 
    datasource_id int8 not null, 
    created_at timestamp not null default current_timestamp, 
primary key(transaction_id)
)".Replace("\r\n", "\n"), sql.Replace("\r\n", "\n"));
	}

	[Fact]
	public void CreateDestinationTable()
	{
		var t = DefinitionRepository.GetDestinationTableDefinition();
		var sql = t.ToCreateTableCommandText();

		Assert.Equal(@"create table if not exists public.destinations (
    destination_id serial8 not null, 
    destination_table_name text not null, 
    db_table_text text not null, 
    sequence_text text not null, 
    validate_option text, 
    flip_option text, 
    delete_option text, 
    created_at timestamp not null default current_timestamp, 
    updated_at timestamp not null default current_timestamp, 
primary key(destination_id)
)".Replace("\r\n", "\n"), sql.Replace("\r\n", "\n"));
	}

	[Fact]
	public void CreateIndex()
	{
		var t = DefinitionRepository.GetDatasourceTableDefinition();

		var sqls = t.ToCreateIndexCommandTexts().ToList();

		Assert.Equal(2, sqls.Count);
		Assert.Equal("create unique index if not exists i1_datasources on public.datasources (datasource_name)", sqls[0]);
		Assert.Equal("create index if not exists i2_datasources on public.datasources (destination_id)", sqls[1]);
	}
}