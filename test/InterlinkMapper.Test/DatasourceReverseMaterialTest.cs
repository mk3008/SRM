using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

namespace InterlinkMapper.Test;

public class DatasourceReverseMaterialTest
{
	public DatasourceReverseMaterialTest(ITestOutputHelper output)
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
	public void CreateKeyRelationInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = material.AsPrivateProxy().CreateKeyRelationInsertQuery();
		var expect = """
INSERT INTO
    sale_journals__key_r_sales (
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
            __additional_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateKeyMapInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = material.AsPrivateProxy().CreateKeyMapInsertQuery();
		var expect = """
INSERT INTO
    sale_journals__key_m_sales (
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
            __additional_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateDestinationInsertQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = ((DatasourceMaterial)material).AsPrivateProxy().CreateDestinationInsertQuery();
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
            t.sale_id
        FROM
            __additional_datasource AS t
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateRelationInsertSelectQuery()
	{
		var material = MaterialRepository.AdditinalMeterial;

		var query = ((DatasourceMaterial)material).AsPrivateProxy().CreateRelationInsertSelectQuery(1, material.KeyRelationTableFullName, material.DatasourceKeyColumns);
		var expect = """
WITH
    material_data AS (
        SELECT
            t.sale_journal_id,
            t.journal_closing_date,
            t.sale_date,
            t.shop_id,
            t.price,
            t.sale_id
        FROM
            __additional_datasource AS t
    )
SELECT
    1 AS interlink_process_id,
    d.sale_journal_id,
    COALESCE(kr.root__sale_journal_id, d.sale_journal_id) AS root__sale_journal_id,
    d.sale_journal_id AS origin__sale_journal_id,
    '' AS interlink_remarks
FROM
    material_data AS d
    LEFT JOIN (
        /* if reverse transfer is performed, one or more rows exist. */
        SELECT
            kr.sale_id,
            kr.sale_journal_id AS root__sale_journal_id
        FROM
            (
                SELECT
                    kr.sale_id,
                    kr.sale_journal_id,
                    ROW_NUMBER() OVER(
                        PARTITION BY
                            kr.sale_id
                        ORDER BY
                            kr.sale_journal_id
                    ) AS row_num
                FROM
                    sale_journals__key_r_sales AS kr
                    INNER JOIN material_data AS d ON kr.sale_id = d.sale_id
            ) AS kr
        WHERE
            kr.row_num = 1
    ) AS kr ON d.sale_id = kr.sale_id
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
