using Carbunql;
using InterlinkMapper.Materializer;
using InterlinkMapper.Models;
using Microsoft.Extensions.Logging;
using PostgresSample;
using Xunit.Abstractions;

namespace AdditionalForwardingTest;

public class MaterializeServiceTest //: IClassFixture<PostgresDB>
{
	public MaterializeServiceTest(ITestOutputHelper output)//(PostgresDB postgresDB, ITestOutputHelper output)
	{
		Logger = new UnitTestLogger(output);

		Environment = new SystemEnvironment()
		{
			DbConnetionConfig = new DummyDB(),
		};
	}

	private readonly UnitTestLogger Logger;

	public readonly SystemEnvironment Environment;

	private MaterializeResult CreateResult()
	{
		var sq = new SelectQuery("select a.id, a.sub_id, 'abc' as text, 1 as value from table_a as a");
		var service = new MaterializeService(Environment);
		return service.AsPrivateProxy().CreateResult("material", 10, sq);
	}

	[Fact]
	public void TestCreateResult()
	{
		var result = CreateResult();

		var expect = """
/* select material table */
SELECT
	d.id,
	d.sub_id,
    d.text,
    d.value
FROM
	material AS d
""";
		var actual = result.SelectQuery.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(10, result.Count);
		Assert.Equal("material", result.MaterialName);
		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}

	[Fact]
	public void TestCreateOriginDeleteQuery()
	{
		var result = CreateResult();

		var service = new MaterializeService(Environment);
		var query = service.AsPrivateProxy().CreateOriginDeleteQuery(result, "requests", ["id", "sub_id"]);

		var expect = """
DELETE FROM
    requests AS d
WHERE
	(d.id, d.sub_id) IN (
		/* Data that has been materialized will be deleted from the original. */
		SELECT
			ot.id,
			ot.sub_id
		FROM
			requests AS ot
		WHERE
			EXISTS (
				SELECT
					*
				FROM
					material AS x
				WHERE
					x.id = ot.id
					AND x.sub_id = ot.sub_id
			)
	)
""";
		var actual = query.ToText();
		Logger.LogInformation(actual);

		Assert.Equal(expect.ToValidateText(), actual.ToValidateText());
	}
}
