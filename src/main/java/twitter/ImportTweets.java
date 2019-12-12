package twitter;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

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
public class ImportTweets {
	private static final String RAW_DIR = "로그/social/twitter_raw";
	private static final String RESULT = "로그/social/twitter";
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

		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", SRID);
		// 질의 처리를 위한 질의 프로그램 생성
		Plan plan = marmot.planBuilder("import_tweets")
							// 'LOG_DIR' 디렉토리에 저장된 Tweet 로그 파일들을 읽는다.
							.load(RAW_DIR)
							// 'coordinates'의 위경도 좌표계를 EPSG:5186으로 변경한 값을
							// 'the_geom' 컬럼에 저장시킨다.
							.transformCrs("the_geom", "EPSG:4326", SRID)
							// 중복된 id의 tweet를 제거시킨다.
							.distinct("id")
							// 'OUTPUT_LAYER'에 해당하는 레이어로 저장시킨다.
							.store(RESULT, FORCE(gcInfo))
							.build();

		// MarmotServer에 생성한 프로그램을 전송하여 수행시킨다.
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		result.cluster();
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
}
