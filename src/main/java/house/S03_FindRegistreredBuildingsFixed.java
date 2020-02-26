package house;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S03_FindRegistreredBuildingsFixed {
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
		
		marmot.close();
	}
	
	static final DataSet process(MarmotRuntime marmot, String buildings, String registry,
									String result)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet input = marmot.getDataSet(buildings);
		String geomCol = input.getGeometryColumn();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();

		Plan plan = Plan.builder("총괄표제부 보유 건물 추출")
						.load(registry, LoadOptions.SPLIT_COUNT(8))
						.knnJoin(geomCol, buildings, 10, 1, "param.*")
						.store(result, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(result);
		ds.createSpatialIndex();
		elapsed.stop();
		
		System.out.printf("총괄표제부 보유 건물 추출 완료, count=%d, elapsed=%s%n",
							ds.getRecordCount(), elapsed.getElapsedMillisString());
		
		return ds;
	}
}
