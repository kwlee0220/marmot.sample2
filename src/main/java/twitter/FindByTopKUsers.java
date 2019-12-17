package twitter;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.Plan;
import marmot.RecordScript;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindByTopKUsers {
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

		String host = MarmotClientCommands.getMarmotHost(cl);
		int port = MarmotClientCommands.getMarmotPort(cl);
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);

		List<String> userIds = findTopKUsers(marmot);
		System.out.println("topk_users=" + userIds);
				
		DataSet info = marmot.getDataSet(TWEETS);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom",
															info.getGeometryColumnInfo().srid());
			
		String userIdsStr = userIds.stream().collect(Collectors.joining(","));
		String inializer = String.format("$target_users = Lists.newArrayList(%s)", userIdsStr);
		String pred = "$target_users.contains(user_id)";
		Plan plan = Plan.builder("find_by_userids")
								.load(TWEETS)
								.filter(RecordScript.of(inializer, pred))
								.project("the_geom,id")
								.store(RESULT, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(RESULT);
		watch.stop();
		
		// 결과에 포함된 일부 레코드를 읽어 화면에 출력시킨다.
		SampleUtils.printPrefix(result, 5);
		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
	}

	public static List<String> findTopKUsers(PBMarmotClient marmot) throws Exception {
		// 가장 자주 tweet을 한 사용자 식별자들을 저장할 임시 파일 이름을 생성한다.
		Plan plan = Plan.builder("list_topk_users")
								.load(TWEETS)
								.aggregateByGroup(Group.ofKeys("user_id"), COUNT())
								.pickTopK("count:D", 5)
								.build();
		try ( RecordSet rset = marmot.executeToRecordSet(plan) ) {
			return rset.fstream().map(r -> r.getString("user_id")).toList();
		}
	}
}
