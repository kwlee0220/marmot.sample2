package podo;

import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.concurrent.CompletableFuture;

import org.apache.log4j.PropertyConfigurator;

import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.plan.LoadOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SplitLandCover {
	private static final String LAND_COVER_1987 = "토지/토지피복도/1987";
	private static final String LAND_COVER_2007 = "토지/토지피복도/2007";
	private static final String OUTPUT_1987_S = "토지/토지피복도/1987S";
	private static final String OUTPUT_2007_S = "토지/토지피복도/2007S";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> split(marmot, LAND_COVER_1987, OUTPUT_1987_S));
		Thread.sleep(120 * 1000);
		split(marmot, LAND_COVER_2007, OUTPUT_2007_S);
		future.join();
		
		System.out.println("완료: 토지피복도 분할");
		System.out.printf("elapsed time=%s%n", watch.getElapsedMillisString());
	}
	
	private static void split(PBMarmotClient marmot, String inputDsId, String outputDsId) {
		DataSet ds = marmot.getDataSet(inputDsId);
		String geomCol = ds.getGeometryColumn();
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan = Plan.builder(inputDsId + "_분할")
							.load(inputDsId, LoadOptions.SPLIT_COUNT(16))
							.assignUid("uid")
							.splitGeometry(geomCol)
							.drop(0)
							.store(outputDsId, FORCE(gcInfo))
							.build();

		StopWatch watch = StopWatch.start();
		marmot.execute(plan);
		watch.stop();
		
		System.out.printf("done: input=%s elapsed=%s%n", inputDsId, watch.getElapsedMillisString());
	}
}
