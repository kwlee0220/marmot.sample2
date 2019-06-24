package appls;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FireStationService {
	private static final String LAND_USAGE = "토지/용도지역지구";
	private static final String POP = "주민/주거인구100m";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		DataSet result;
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
		String geomCol = ds.getGeometryColumn();
		
		plan = marmot.planBuilder("combine")
					.load(LAND_USAGE)
					.filter("lclas_cl=='UQA100'")
					.spatialJoin(geomCol, POP, "param.{the_geom as the_geom,인구수 as pop}")
					.store(RESULT)
					.build();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
