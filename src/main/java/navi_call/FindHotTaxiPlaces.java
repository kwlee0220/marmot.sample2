package navi_call;

import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.DataSetOption;
import marmot.Plan;
import marmot.command.MarmotClient;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotTaxiPlaces {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String EMD = "시연/서울_읍면동";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("mc_list_records ");
		parser.addArgOption("host", "ip_addr", "marmot server host (default: localhost)", false);
		parser.addArgOption("port", "number", "marmot server port (default: 12985)", false);
		
		CommandLine cl = parser.parseArgs(args);
		if ( cl.hasOption("help") ) {
			cl.exitWithUsage(0);
		}

		String host = MarmotClient.getMarmotHost(cl);
		int port = MarmotClient.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Plan plan = marmot.planBuilder("find_hot_taxi_places")
							.load(TAXI_LOG)
							.filter("status==1 || status==2")
							.spatialJoin("the_geom", EMD,
										"car_no,status,ts,param.{the_geom, EMD_CD,EMD_KOR_NM}")
							.expand1("hour:int", "ts.substring(8,10)")
							.groupBy("hour,status,EMD_CD")
								.tagWith("EMD_KOR_NM,the_geom")
								.aggregate(COUNT())
							.filter("count > 50")
							.groupBy("hour,status")
								.orderBy("count:D")
								.list()
							.store(RESULT)
							.build();
		DataSet result = marmot.createDataSet(RESULT, plan, DataSetOption.FORCE);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 50);
	}
}