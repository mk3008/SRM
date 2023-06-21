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
		ObjectRelationMapper.AddTypeHandler(destdef);
		ObjectRelationMapper.AddTypeHandler(sourcedef);
		ObjectRelationMapper.AddTypeHandler(LoadTestDefinitions.GetTextFileDefinition());
		ObjectRelationMapper.AddTypeHandler(LoadTestDefinitions.GetTextFolderDefinition());

		//Dapper
		SqlMapper.AddTypeHandler(new JsonTypeHandler<DbTable>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<Sequence?>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<DbTableDefinition?>());
		SqlMapper.AddTypeHandler(new JsonTypeHandler<List<string>>());
		SqlMapper.AddTypeHandler(new ValidateOptionTypeHandler());
		SqlMapper.AddTypeHandler(new FlipOptionTypeHandler());
		SqlMapper.AddTypeHandler(new DeleteOptionTypeHandler());
	}
}