using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.LoadTestModels;
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
	public void Insert()
	{
		using var cn = (new PostgresDB()).ConnectionOpenAsNew();
		using var trn = cn.BeginTransaction();

		cn.CreateTableOrDefault<TextFile>();
		cn.CreateTableOrDefault<TextFolder>();
		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var folder = new TextFolder() { TextFolderName = "test " + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") };
		ac.Save(cn, folder);

		var file1 = new TextFile() { TextFolder = folder, TextFileName = "file1" };
		ac.Save(cn, file1);

		var file2 = new TextFile() { TextFolder = folder, TextFileName = "file2" };
		ac.Save(cn, file2);

		trn.Commit();
	}

	[Fact]
	public void Select()
	{
		using var cn = (new PostgresDB()).ConnectionOpenAsNew();

		cn.CreateTableOrDefault<TextFile>();
		cn.CreateTableOrDefault<TextFolder>();
		//var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var mapper = new SelectQueryMapper<TextFile>();

		var lst = mapper.Load(cn);
	}
}
