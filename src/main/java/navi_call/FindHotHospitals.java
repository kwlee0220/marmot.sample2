package navi_call;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.plan.Group;
import marmot.plan.SpatialJoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindHotHospitals {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String HOSPITAL = "시연/hospitals";
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

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch;
		watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		Plan plan = marmot.planBuilder("find_hot_hospitals")
								.load(TAXI_LOG)
								.filter("status==1 || status==2")
								.spatialJoin("the_geom", HOSPITAL,
											SpatialJoinOptions.WITHIN_DISTANCE(50)
															.outputColumns("param.{the_geom,gid,bplc_nm,bz_stt_nm}"))
								.filter("bz_stt_nm=='운영중'")
								.aggregateByGroup(Group.ofKeys("gid").withTags("the_geom,bplc_nm"),
												COUNT())
								.rank("count:D", "rank")
								.store(RESULT, FORCE)
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 50);
	}
}
