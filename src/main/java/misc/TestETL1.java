package misc;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.AggregateFunction;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestETL1 {
//	private static final String INPUT = "tmp/dtg1";
//	private static final String INPUT = "tmp/dtg2";
//	private static final String INPUT = "tmp/building1";
//	private static final String INPUT = "tmp/building2";
//	private static final String INPUT = "tmp/hospital1";
//	private static final String INPUT = "tmp/hospital2";
	private static final String RESULT = "tmp/result";
	
	private static String INPUT;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		Plan plan = Plan.builder("test_dtg1")
						.load(INPUT)
						.buffer("the_geom", 50)
						.aggregate(AggregateFunction.ENVELOPE("the_geom"))
//						.expand("the_geom:polygon", "the_geom = ST_GeomFromEnvelope(mbr)")
						.store(RESULT, FORCE)
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
}
