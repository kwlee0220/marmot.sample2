package house;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S04_FindHouseCandidates {
	private static final String HOUSE_CADASTRAL = "tmp/house/house_cadastral";
	private static final String REG_BUILDINGS = "tmp/house/registered_buildings";
	private static final String RESULT = "tmp/house/house_candidates";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		// 전국 건물 중에서 총괄표제부 보유한 건물 추출
		process(marmot, HOUSE_CADASTRAL, REG_BUILDINGS, RESULT);
		
		marmot.close();
	}
	
	static final DataSet process(MarmotRuntime marmot, String houseCadastral,
								String registeredBuildings, String result)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet input = marmot.getDataSet(houseCadastral);
		String geomCol = input.getGeometryColumn();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("총괄표제부 건물영역 제외 주거지적 영역 추출")
						.load(houseCadastral)
						.differenceJoin(geomCol, registeredBuildings)
						.store(result)
						.store(result, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(result);

		elapsed.stop();
		System.out.printf("총괄표제부 건물영역 제외 주거지적 영역 추출 완료, "
							+ "count=%d, elapsed=%s%n", ds.getRecordCount(),
							elapsed.getElapsedMillisString());
		
		return ds;
	}
}
