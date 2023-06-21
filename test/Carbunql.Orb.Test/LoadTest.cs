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
		var ac = new DbAccessor() { PlaceholderIdentifer = ":", Logger = Logger };

		var sql = @"select 
    t0.text_file_id as t0_TextFileId,
    t0.text_file_name as t0_TextFileName,
    t1.text_folder_id as t1_TextFolderId,
    t1.text_folder_name as t1_TextFolderName
from 
    text_files t0
    inner join text_folders t1 on t0.text_folder_id = t1.text_folder_id";

		using var r = cn.ExecuteReader(sql);

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

		var headers = new List<string>();
		var lst = new List<TextFile>();
		var cash = new Dictionary<(Type, long), object>();
		while (r.Read())
		{
			if (!headers.Any())
			{
				for (int i = 0; i < r.FieldCount - 1; i++) headers.Add(r.GetName(i));
			}

			var instancemaps = new List<InstanceMap>();
			foreach (var typemap in typemaps)
			{
				instancemaps.Add(new() { TypeMap = typemap, Item = Activator.CreateInstance(typemap.Type)! });
			}

			//object mapping
			foreach (var instancemap in instancemaps)
			{
				var tp = instancemap.TypeMap.Type;
				var seqPropName = ObjectRelationMapper.FindFirst(tp).GetSequence().Identifer;
				var seqProp = tp.GetProperty(seqPropName)!;

				foreach (var columnmap in instancemap.TypeMap.ColumnMaps)
				{
					var prop = tp.GetProperty(columnmap.PropertyName)!;
					var val = r[columnmap.ColumnName];
					prop.SetValue(instancemap.Item, val);

					if (prop == seqProp && val == null)
					{
						instancemap.Item = null;
						break;
					}
					if (prop == seqProp)
					{
						var key = (tp, (long)val);
						if (cash.ContainsKey(key))
						{
							instancemap.Item = cash[key];
							break;
						}
						else
						{
							cash[key] = instancemap.Item!;
						}
					}
				}
			}

			//relation
			foreach (var instancemap in instancemaps)
			{
				var rmap = instancemap.TypeMap.RelationMap;
				if (rmap == null) continue;

				var owner = instancemaps.Where(x => x.TypeMap.TableAlias == rmap.OwnerTableAlias).First();
				if (owner.Item == null) continue;

				var prop = owner.TypeMap.Type.GetProperty(rmap.OwnerPropertyName)!;
				prop.SetValue(owner.Item, instancemap.Item);
			}

			var root = instancemaps.Where(x => x.TypeMap.RelationMap == null).Select(x => x.Item).First();
			if (root == null) continue;
			lst.Add((TextFile)root);
		}
	}
}

public class TypeMap
{
	public string TableAlias { get; set; }

	public Type Type { get; set; }

	public RelationMap RelationMap { get; set; }

	public List<ColumnMap> ColumnMaps { get; set; } = new();
}

public class RelationMap
{
	public string OwnerTableAlias { get; set; }

	public string OwnerPropertyName { get; set; }
}

public class ColumnMap
{
	public string PropertyName { get; set; }

	public string ColumnName { get; set; }
}

public class InstanceMap
{
	public TypeMap TypeMap { get; set; }

	public object? Item { get; set; }

	public string TableAlias => TypeMap.TableAlias;
}