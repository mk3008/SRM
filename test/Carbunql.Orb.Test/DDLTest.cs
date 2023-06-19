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
	public void CreateIndex()
	{
		var t = DefinitionRepository.GetDatasourceTableDefinition();

		var sqls = t.ToCreateIndexCommandTexts().ToList();

		Assert.Equal(2, sqls.Count);
		Assert.Equal("create unique index if not exists i1_datasources on public.datasources (datasource_name)", sqls[0]);
		Assert.Equal("create index if not exists i2_datasources on public.datasources (destination_id)", sqls[1]);
	}
}