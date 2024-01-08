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

	public InterlinkProcess ProcessRow => SystemRepository.GetDummyProcess(DatasourceRepository.sales);

	[Fact]
	public void CreateRelationInsertQuery()
	{
		var relation = ProcessRow.InterlinkDatasource.GetKeyRelationTable(Environment);
		var keys = ProcessRow.InterlinkDatasource.KeyColumns.Select(x => x.ColumnName).ToList();

		var material = MaterialRepository.DatasourceReverseMaterial;
		var query = ((DatasourceMaterial)material).AsPrivateProxy().CreateRelationInsertQuery(1, relation.Definition.TableFullName, keys);

		var expect = """
INSERT INTO
    sale_journals__relation (
        interlink_process_id, sale_journal_id, root__sale_journal_id, origin__sale_journal_id, interlink_remarks
    )
WITH
    material_data AS (
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
    )
SELECT
    1 AS interlink_process_id,
    d.sale_journal_id,
    COALESCE(kr.root__sale_journal_id, d.sale_journal_id) AS root__sale_journal_id,
    d.origin__sale_journal_id,
    d.interlink_remarks
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

	[Fact]
	public void CreateDestinationInsertQuery()
	{
		var material = MaterialRepository.DatasourceReverseMaterial;
		var query = ((DatasourceMaterial)material).AsPrivateProxy().CreateDestinationInsertQuery();

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
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateKeyRelationInsertQuery()
	{
		var material = MaterialRepository.DatasourceReverseMaterial;
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
    ) AS d
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void CreateKeyMapDeleteQuery()
	{
		var material = MaterialRepository.DatasourceReverseMaterial;
		var query = material.AsPrivateProxy().CreateKeyMapDeleteQuery();

		var expect = """
DELETE FROM
    sale_journals__key_m_sales AS d
WHERE
    (d.sale_journal_id) IN (
        SELECT
            d.origin__sale_journal_id AS sale_journal_id
        FROM
            (
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
            ) AS d
    )
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
