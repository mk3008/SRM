using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class ReverseDatasourceMaterialTest
{
	public ReverseDatasourceMaterialTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	[Fact]
	public void CreateSelectQuery()
	{
		var material = MaterialRepository.ReverseMeterial;

		var query = material.AsPrivateProxy().CreateSelectQuery(DatasourceRepository.sales);
		var expect = """
/* filterd by datasource */
SELECT
    d.sale_journal_id,
    d.root__sale_journal_id,
    d.origin__sale_journal_id,
    d.journal_closing_date,
    d.sale_date,
    d.shop_id,
    d.price,
    d.remarks,
    d.interlink_datasource_id,
    d.interlink_remarks,
    keymap.sale_id
FROM
    (
        SELECT
            t.sale_journal_id,
            t.root__sale_journal_id,
            t.origin__sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.remarks,
            t.interlink_datasource_id,
            t.interlink_remarks
        FROM
            __reverse_datasource AS t
    ) AS d
    INNER JOIN sale_journals__key_m_sales AS keymap ON d.origin__sale_journal_id = keymap.sale_journal_id
WHERE
    d.interlink_datasource_id = 1
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

}
