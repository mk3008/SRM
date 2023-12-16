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

		Proxy = new ReverseMaterializer(Environment).AsPrivateProxy();
		MaterialRepository = new DummyMaterialRepository(Environment);
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	public readonly DummyMaterialRepository MaterialRepository;

	public readonly ReverseMaterializerProxy Proxy;

	//	[Fact]
	//	public void KeyMap_CreateInsertQueryFrom()
	//	{
	//		var material = MaterialRepository.AdditinalMeterial;
	//		var query = material.AsPrivateProxy().CreateKeyMapInsertQuery();

	//		var expect = """
	//INSERT INTO
	//    sale_journals__km_sales (
	//        sale_journal_id, sale_id
	//    )
	//SELECT
	//    d.sale_journal_id,
	//    d.sale_id
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.journal_closing_date,
	//            t.sale_date,
	//            t.shop_id,
	//            t.price,
	//            t.sale_id,
	//            t.root__sale_journal_id,
	//            t.origin__sale_journal_id,
	//            t.interlink__remarks
	//        FROM
	//            __datasource AS t
	//    ) AS d
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}

	//	[Fact]
	//	public void Reverse_CreateInsertQueryFrom()
	//	{
	//		var material = MaterialRepository.ReverseMeterial;
	//		var query = material.AsPrivateProxy().CreateReverseInsertQuery();

	//		var expect = """
	//INSERT INTO
	//    sale_journals__reverse (
	//        sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink__remarks
	//    )
	//SELECT
	//    d.sale_journal_id,
	//    d.root__sale_journal_id,
	//    d.origin__sale_journal_id,
	//    d.interlink__remarks
	//FROM
	//    (
	//        SELECT
	//            t.sale_journal_id,
	//            t.root__sale_journal_id,
	//            t.origin__sale_journal_id,
	//            t.journal_closing_date,
	//            t.sale_date,
	//            t.shop_id,
	//            t.price,
	//            t.remarks,
	//            t.keymap_name,
	//            t.interlink__remarks
	//        FROM
	//            __reverse_datasource AS t
	//    ) AS d
	//""";
	//		var actual = query.ToText();
	//		Logger.LogInformation(actual);

	//		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	//	}
}
