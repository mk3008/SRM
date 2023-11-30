using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class MaterialInsertableTest
{
	public MaterialInsertableTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};

		Proxy = new ReverseForwardingMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseForwardingMaterializerProxy Proxy;

	[Fact]
	public void Keymap_CreateInsertQueryFrom()
	{
		var datasource = DatasourceRepository.sales;
		var keymap = Environment.GetKeymapTable(datasource);
		var datasourceMaterial = MaterialRepository.AdditinalDatasourceMeterial;

		var query = keymap.CreateInsertQueryFrom(datasourceMaterial);

		var expect = """
INSERT INTO
    sale_journals__m_sales (
        sale_journal_id, sale_id
    )
SELECT
    d.sale_journal_id,
    d.sale_id
FROM
    (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void Reverse_CreateInsertQueryFrom()
	{
		var destination = DestinationRepository.sale_journals;
		var reverse = Environment.GetReverseTable(destination);
		var datasourceMaterial = MaterialRepository.ReverseDatasourceMeterial;

		var query = reverse.CreateInsertQueryFrom(datasourceMaterial);

		var expect = """
INSERT INTO
    sale_journals__reverse (
        sale_journal_id, origin__sale_journal_id, interlink__remarks
    )
SELECT
    d.sale_journal_id,
    d.origin__sale_journal_id,
    d.interlink__remarks
FROM
    (
        SELECT
            t.sale_journal_id,
            t.origin__sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.remarks,
            t.keymap_name,
            t.interlink__remarks
        FROM
            __reverse_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
