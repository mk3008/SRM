using Carbunql.Dapper;
using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.DBTestModels;
using Carbunql.Orb.Test.LoadTestModels;
using Dapper;
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

		//cn.CreateTableOrDefault<TextFile>();
		//cn.CreateTableOrDefault<TextFolder>();
		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var sql = @"select 
    t.text_file_id as TextFileId,
    t.text_file_name as TextFileName,
    t1.text_folder_id as TextFolderId,
    t1.text_folder_name as TextFolderName
from 
    text_files t
    inner join text_folders t1 on t.text_folder_id = t1.text_folder_id";

		var lst = cn.Query<TextFile>(sql, types: new[] { typeof(TextFile), typeof(TextFolder) }, map: Mapper(), splitOn: "TextFolderId").ToList();

		//var lst = cn.Query<TextFile, TextFolder, TextFile>(sql, (x, y) =>
		//{
		//	x.TextFolder = y;
		//	return x;
		//}, splitOn: "TextFolderId").ToList();
	}

	private Func<object[], TextFile> Mapper()
	{
		return (obj) =>
		{
			var root = (TextFile)obj[0];
			root.GetType().GetProperty("TextFolder")!.SetValue(root, obj[1]);
			return root;
		};
	}
}
