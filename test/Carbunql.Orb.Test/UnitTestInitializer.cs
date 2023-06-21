using Carbunql.Orb.Test.DBTestModels;
using Carbunql.Orb.Test.LoadTestModels;
using Dapper;
using System.Runtime.CompilerServices;

namespace Carbunql.Orb.Test;

internal static class UnitTestInitializer
{
	[ModuleInitializer]
	public static void Initialize()
	{
		//Carbunql.Orb
		var destdef = DefinitionRepository.GetDestinationTableDefinition();
		var sourcedef = DefinitionRepository.GetDatasourceTableDefinition();
		ObjectTableMapper.Add(destdef);
		ObjectTableMapper.Add(sourcedef);
		ObjectTableMapper.Add(LoadTestDefinitions.GetTextFileDefinition());
		ObjectTableMapper.Add(LoadTestDefinitions.GetTextFolderDefinition());

		//Dapper
		SqlMapper.AddTypeHandler(new JsonTypeHandler<DbTable>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<Sequence?>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<DbTableDefinition?>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<List<string>>());
		SqlMapper.AddTypeHandler(new ValidateOptionTypeHandler());
		SqlMapper.AddTypeHandler(new FlipOptionTypeHandler());
		SqlMapper.AddTypeHandler(new DeleteOptionTypeHandler());
		SqlMapper.AddTypeHandler(new ObjectTableMappableTypeHandler<TextFile>());
		SqlMapper.AddTypeHandler(new ObjectTableMappableTypeHandler<TextFolder>());
	}
}