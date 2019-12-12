package navi_call.map;

import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.INTERSECTS;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import navi_call.Globals;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S2_FindDongTaxiLog {
	private static final String INPUT = Globals.TAXI_LOG;
	private static final String RESULT = Globals.TAXI_LOG_DONG;
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Geometry guBoundary = getDongBoundary(marmot, Globals.DONG);
		
		DataSet input = marmot.getDataSet(INPUT);
		String geomCol = input.getGeometryColumn();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("동내_로그_추출")
					.load(INPUT)
					.filterSpatially(geomCol, INTERSECTS, guBoundary)
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();

		System.out.printf("count=%d elapsed=%s%n", result.getRecordCount(),
													watch.getElapsedMillisString());
	}
	
	private static Geometry getDongBoundary(MarmotRuntime marmot, String dongName)
		throws Exception {
		String predicate = String.format("emd_kor_nm == '%s'", dongName);
		Plan plan = marmot.planBuilder("filter")
							.load(Globals.EMD)
							.filter(predicate)
							.project("the_geom")
							.build();
		return marmot.executeLocally(plan)
						.fstream()
						.map(rec -> rec.getGeometry(0))
						.findFirst()
						.getOrNull();
	}
}
