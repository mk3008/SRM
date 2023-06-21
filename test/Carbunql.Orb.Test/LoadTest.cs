using Carbunql.Dapper;
using Carbunql.Orb.Extensions;
using Carbunql.Orb.Test.LoadTestModels;
using Dapper;
using System.Data;
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

		// TODO : auto generated
		var sql = @"select 
    t0.text_file_id as t0_TextFileId,
    t0.text_file_name as t0_TextFileName,
    t1.text_folder_id as t1_TextFolderId,
    t1.text_folder_name as t1_TextFolderName
from 
    text_files t0
    inner join text_folders t1 on t0.text_folder_id = t1.text_folder_id";

		// TODO : auto generated
		var typemaps = new List<TypeMap>
		{
			new ()
			{
				TableAlias = "t0",
				Type = typeof(TextFile),
				ColumnMaps = new()
				{
					new () {ColumnName = "t0_TextFileId", PropertyName= "TextFileId" },
					new () {ColumnName = "t0_TextFileName", PropertyName= "TextFileName" },
				}
			},
			new ()
			{
				TableAlias = "t1",
				Type = typeof(TextFolder),
				RelationMap = new() { OwnerTableAlias = "t0", OwnerPropertyName = "TextFolder" },
				ColumnMaps = new()
				{
					new () {ColumnName = "t1_TextFolderId", PropertyName= "TextFolderId" },
					new () {ColumnName = "t1_TextFolderName", PropertyName= "TextFolderName" },
				}
			}
		};

		var mapper = new SelectQueryMapper<TextFile>()
		{
			SelectQuery = new(sql),
			TypeMaps = typemaps,
		};

		var lst = mapper.Load(cn);
	}
}
