package navi_call;

import static marmot.optor.StoreDataSetOptions.FORCE;
import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindPassingStation {
	private static final String TAXI_TRJ = "로그/나비콜/택시경로";
	private static final String RESULT = "tmp/result";
	private static final String SRID = "EPSG:5186";

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
//		KryoMarmotClient marmot = KryoMarmotClient.connect(host, port);

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", SRID);
		Geometry key = getSubwayStations(marmot, "사당역");
		Plan plan = Plan.builder("find_passing_station")
							.load(TAXI_TRJ)
							.filter("status == 3")
							.defineColumn("the_geom:line_string", "ST_TRLineString(trajectory)")
							.filterSpatially("the_geom", WITHIN_DISTANCE(100), key)
							.project("*-{trajectory}")
							.store(RESULT, FORCE(gcInfo))
							.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		
		SampleUtils.printPrefix(result, 5);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}

	private static final String SUBWAY_STATIONS = "교통/지하철/서울역사";
	private static Geometry getSubwayStations(PBMarmotClient marmot, String stationName)
		throws Exception {
		String predicate = String.format("kor_sub_nm == '%s'", stationName);
		Plan plan = Plan.builder("filter_subway_stations")
							.load(SUBWAY_STATIONS)
							.filter(predicate)
							.project("the_geom")
							.build();
		return marmot.executeLocally(plan)
						.fstream()
						.map(rec -> rec.getGeometry(0))
						.findFirst().getOrNull();
	}
}
