package misc;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestETL2 {
//	private static final String INPUT = "tmp/dtg1";
//	private static final String INPUT = "tmp/dtg2";
//	private static final String INPUT = "tmp/building1";
//	private static final String INPUT = "tmp/building2";
//	private static final String INPUT = "tmp/hospital1";
//	private static final String INPUT = "tmp/hospital2";
	private static final String STATIONS = "교통/지하철/출입구";
	private static final String PARAM = "tmp/buffered";
	private static final String RESULT = "tmp/result";
	
	private static String INPUT;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		INPUT = args[0];
		
//		bufferStations(marmot);
		System.out.println("dataset=" + INPUT);
		
		Plan plan = marmot.planBuilder("test_dtg")
						.load(INPUT)
						.spatialJoin("the_geom", PARAM, "the_geom,param.sub_sta_sn")
						.aggregateByGroup(Group.ofKeys("sub_sta_sn"), COUNT())
						.store(RESULT, FORCE)
						.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}
	
	private static void bufferStations(PBMarmotClient marmot) {
		GeometryColumnInfo gcInfo = marmot.getDataSet(STATIONS).getGeometryColumnInfo();
		Plan plan = marmot.planBuilder("buffer")
						.load(STATIONS)
						.buffer("the_geom", 100)
						.store(PARAM, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet result = marmot.getDataSet(PARAM);
		result.cluster();
	}
}
