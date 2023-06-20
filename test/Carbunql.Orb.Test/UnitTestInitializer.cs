using Carbunql.Orb.Test.DBTestModels;
using Dapper;
using System.Runtime.CompilerServices;

namespace Carbunql.Orb.Test;

internal static class UnitTestInitializer
{
	[ModuleInitializer]
	public static void Initialize()
	{
		//Dapper
		SqlMapper.AddTypeHandler(new DbTableTypeHandler());
		SqlMapper.AddTypeHandler(new SequenceTypeHandler());
		SqlMapper.AddTypeHandler(new DbTableDefinitionTypeHandler());
		SqlMapper.AddTypeHandler(new ListStringTypeHandler());
		SqlMapper.AddTypeHandler(new ValidateOptionTypeHandler());
		SqlMapper.AddTypeHandler(new FlipOptionTypeHandler());
		SqlMapper.AddTypeHandler(new DeleteOptionTypeHandler());

		//Carbunql.Orb
		var destdef = DefinitionRepository.GetDestinationTableDefinition();
		var sourcedef = DefinitionRepository.GetDatasourceTableDefinition();
		ObjectTableMapper.Add(destdef);
		ObjectTableMapper.Add(sourcedef);
	}
}