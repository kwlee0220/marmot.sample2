package misc;

import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final String GRID = "tmp/test2017/grid30";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan;
		DataSet result;
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", "EPSG:5186");
		
		plan = marmot.planBuilder("xx")
					.load("tmp/hcode")
					.update("the_geom = ST_GeomFromEnvelope(ST_AsEnvelope(the_geom))")
					.store("tmp/hcode2", FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet("tmp/hcode2");
		
		plan = marmot.planBuilder("yy")
					.load("tmp/cada")
					.update("the_geom = ST_GeomFromEnvelope(ST_AsEnvelope(the_geom))")
					.store("tmp/cada2", FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet("tmp/cada2");

		GeometryColumnInfo gcInfo2 = new GeometryColumnInfo("the_geom", "EPSG:4326");
		plan = marmot.planBuilder("find_closest_point_on_link")
					.load("tmp/cada2")
					.spatialJoin("the_geom", "tmp/hcode2",
								"the_geom,pnu,param.the_geom as the_geom2, param.hcode")
					.defineColumn("the_geom:point",
							"ST_Centroid(the_geom.intersection(the_geom2))")
					.transformCrs("the_geom", "EPSG:5186", "EPSG:4326")
					.store(RESULT, FORCE(gcInfo2))
					.build();
		marmot.execute(plan);
		result = marmot.getDataSet(RESULT);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
