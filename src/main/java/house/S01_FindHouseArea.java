package house;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S01_FindHouseArea {
	private static final String LAND_USAGE = "토지/용도지역지구_추진단";
	private static final String RESULT = "tmp/house/house_area";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		// 용도지구에서 주거 지역 추출
		process(marmot, LAND_USAGE, RESULT);
		
		marmot.close();
	}

	static final DataSet process(MarmotRuntime marmot, String landUsage, String result)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet ds = marmot.getDataSet(landUsage);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();

		Plan plan = marmot.planBuilder("주거지역 추출")
						.load(landUsage)
						.filter("lclas_cl == 'UQA100'")
						.store(landUsage, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet resDs = marmot.getDataSet(landUsage);
		resDs.cluster();
		System.out.printf("용도지역지구에서 주거지역 추출 완료, count=%d, elapsed=%s%n",
							resDs.getRecordCount(), elapsed.getElapsedMillisString());
		
		return resDs;
	}
}
