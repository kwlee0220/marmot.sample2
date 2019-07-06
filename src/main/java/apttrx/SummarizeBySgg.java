package apttrx;

import static marmot.StoreDataSetOptions.*;
import static marmot.optor.AggregateFunction.AVG;
import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.AggregateFunction.SUM;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SummarizeBySgg {
	private static final String APT_LOC = "주택/실거래/아파트위치";
	private static final String APT_TRX = "주택/실거래/아파트매매";
	private static final String SGG = "구역/시군구";
	private static final String RESULT = "tmp/아파트실매매/시군구별";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		Plan plan;
		DataSet emd = marmot.getDataSet(SGG);
		String geomCol = emd.getGeometryColumn();
		
		plan = marmot.planBuilder("summarize_by_station")
						.load(APT_TRX)
						.hashJoin("시군구,번지,단지명", APT_LOC,
								"시군구,번지,단지명", "*,param.{info}", JoinOptions.INNER_JOIN)
						.expand("평당거래액:int,sgg_cd:string",
								"평당거래액 = (int)Math.round((거래금액*3.3) / 전용면적);"
										+ "sgg_cd = info.getSggCode();")
						.aggregateByGroup(Group.ofKeys("sgg_cd"), 
										COUNT().as("거래건수"),
										SUM("거래금액").as("총거래액"),
										AVG("평당거래액").as("평당거래액"),
										MAX("거래금액").as("최대거래액"),
										MIN("거래금액").as("최소거래액"))
						.defineColumn("평당거래액:int")
						
						.hashJoin("sgg_cd", SGG, "sig_cd",
									String.format("*,param.{%s,sig_kor_nm}", geomCol),
									JoinOptions.INNER_JOIN)
						.sort("거래건수:D")
						
						.store(RESULT)
						.build();
		GeometryColumnInfo gcInfo = emd.getGeometryColumnInfo();
		marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		watch.stop();
		
		System.out.printf("elapsed: %s%n", watch.getElapsedMillisString());
	}
}
