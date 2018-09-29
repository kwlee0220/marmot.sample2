package twitter;

import static marmot.DataSetOption.FORCE;

import org.apache.log4j.PropertyConfigurator;

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
public class GroupByWeekDay {
	private static final String TWEETS = "로그/social/twitter";
	private static final String RESULT = "/tmp/result";

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

		Plan plan = marmot.planBuilder("group_by_weekday_and_count")
								.load(TWEETS)
								.project("id,created_at")
								.expand("week_day:int", "week_day = ST_DTWeekDay(created_at)")
								.groupBy("week_day").count()
								.drop(0)
								.store(RESULT)
								.build();
		marmot.createDataSet(RESULT, plan, FORCE);
		
		watch.stop();
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
}
