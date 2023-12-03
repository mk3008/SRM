using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class DestinationTest
{
	public DestinationTest(ITestOutputHelper output)
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
	public void CreateInsertQueryFromAdditionalMaterial()
	{
		var destination = DestinationRepository.sale_journals;
		var datasourceMaterial = MaterialRepository.AdditinalDatasourceMeterial;

		var query = destination.CreateInsertQueryFrom(datasourceMaterial);

		var expect = """
	INSERT INTO
	    sale_journals (
	        sale_journal_id, journal_closing_date, sale_date, shop_id, price
	    )
	SELECT
	    d.sale_journal_id,
	    d.journal_closing_date,
	    d.sale_date,
	    d.shop_id,
	    d.price
	FROM
	    (
	        SELECT
	            t.sale_journal_id,
	            t.journal_closing_date,
	            t.sale_date,
	            t.shop_id,
	            t.price,
	            t.sale_id,
				t.root__sale_journal_id,
				t.origin__sale_journal_id
	        FROM
	            __datasource AS t
	    ) AS d
	""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateInsertQueryFromReverseMaterial()
	{
		var destination = DestinationRepository.sale_journals;
		var datasourceMaterial = MaterialRepository.ReverseDatasourceMeterial;

		var query = destination.CreateInsertQueryFrom(datasourceMaterial);

		var expect = """
	INSERT INTO
	    sale_journals (
	        sale_journal_id, journal_closing_date, sale_date, shop_id, price, remarks
	    )
	SELECT
	    d.sale_journal_id,
	    d.journal_closing_date,
	    d.sale_date,
	    d.shop_id,
	    d.price,
	    d.remarks
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
