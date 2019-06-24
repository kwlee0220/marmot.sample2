package house;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_FindRegistreredBuildings {
	private static final String BUILDINGS = "주소/건물_추진단";
	private static final String REGISTRY = "건물/건축물대장/총괄표제부";
	private static final String RESULT = "tmp/house/registered_buildings";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		// 전국 건물 중에서 총괄표제부 보유한 건물 추출
		process(marmot, BUILDINGS, REGISTRY, RESULT);
		
		marmot.disconnect();
	}
	
	static final DataSet process(MarmotRuntime marmot, String buildings, String registry,
									String resultId)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(buildings);
		String geomCol = ds.getGeometryColumn();

		Plan plan = marmot.planBuilder("총괄표제부 보유 건물 추출")
						.load(buildings)
						.arcSpatialJoin(geomCol, registry, true, true)
						.store(resultId)
						.build();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		DataSet result = marmot.createDataSet(resultId, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		result.cluster();
		elapsed.stop();
		
		System.out.printf("총괄표제부 보유 건물 추출 완료, count=%d, elapsed=%s%n",
							result.getRecordCount(), elapsed.getElapsedMillisString());
		
		return result;
	}
}
