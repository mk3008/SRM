using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.DBTestModels;
using Carbunql.Orb.Test.LoadTestModels;
using System.ComponentModel.DataAnnotations;
using System.Diagnostics.CodeAnalysis;
using Xunit.Abstractions;

namespace Carbunql.Orb.Test;

public class LoadTest
{
	public LoadTest(ITestOutputHelper output)
	{
		Logger = new UnitTestLogger() { Output = output };
	}

	private readonly UnitTestLogger Logger;

	[Fact]
	public void Execute()
	{
		using var cn = (new PostgresDB()).ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		cn.CreateTableOrDefault<TextFile>();
		cn.CreateTableOrDefault<TextFolder>();
		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var folder = new TextFolder() { TextFolderName = "test " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") };
		ac.Save(cn, folder);

		var file = new TextFile() { TextFolder = folder, TextFileName = "file" };
		ac.Save(cn, file);

		trn.Commit();
	}
}
