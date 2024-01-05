using Carbunql.Extensions;
using Dapper;
using InterlinkMapper.Models;
using RedOrb;
using RedOrb.Attributes;
using System.Reflection.Metadata;
using System.Runtime.CompilerServices;

namespace InterlinkMapper.Test;

internal static class UnitTestInitializer
{
	[ModuleInitializer]
	public static void Initialize()
	{
		var env = new SystemEnvironment();
		env.DbTableConfig.ControlTableSchemaName = "interlink";

		var cnv = new Converter { Environment = env };

		//RedOrb setting
		ObjectRelationMapper.Converter = cnv.Convert;
		ObjectRelationMapper.PlaceholderIdentifer = env.DbEnvironment.PlaceHolderIdentifer;
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkDestination>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkDatasource>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkTransaction>());
		ObjectRelationMapper.AddTypeHandler(DefinitionBuilder.Create<InterlinkProcess>());

		//Dapper setting
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<Sequence>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<List<KeyColumn>>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<ReverseOption>());
		CustomTypeMapper.AddTypeHandler(new JsonTypeHandler<DbTable>());
	}

	private class Converter
	{
		public required SystemEnvironment Environment { get; set; }

		public DbTableDefinition Convert(DbTableDefinition def)
		{
			//override schema
			def.SchemaName = Environment.DbTableConfig.ControlTableSchemaName;

			//override type
			foreach (var item in def.ColumnDefinitions)
			{
				if (item.IsAutoNumber)
				{
					item.ColumnType = Environment.DbEnvironment.AutoNumberTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("numeric"))
				{
					item.ColumnType = Environment.DbEnvironment.NumericTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("text"))
				{
					item.ColumnType = Environment.DbEnvironment.TextTypeName;
				}
				else if (item.ColumnType.IsEqualNoCase("timestamp"))
				{
					item.ColumnType = Environment.DbEnvironment.TimestampTypeName;
				}
			}
			return def;
		}
	}
}
