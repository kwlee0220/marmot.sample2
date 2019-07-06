package appls;

import static marmot.StoreDataSetOptions.*;
import static marmot.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Geometry;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.optor.JoinOptions;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FilterInSeoul {
	private static final String SID = "구역/시도";
	private static final String LAND_USAGE = "토지/토지이용계획_누적";
	private static final String CADASTRAL = "구역/연속지적도";
	private static final String CADASTRAL_SEOUL = "tmp/seoul";
	private static final String RESULT = "tmp/result";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		Plan plan;
		DataSet result;
		
		DataSet ds = marmot.getDataSet(LAND_USAGE);
//		String geomCol = ds.getGeometryColumn();
//		String srid = ds.getSRID();
		
		// 전국 시도 행정구역 데이터에서 서울특별시 영역만을 추출한다.
		Geometry seoul = getSeoulBoundary(marmot);
		getSeoulCadastral(marmot, seoul, CADASTRAL_SEOUL);
		
		plan = marmot.planBuilder("tag_geom")
					.load(LAND_USAGE)
					.filter("법정동코드.startsWith('11')")
					.hashJoin("고유번호", CADASTRAL_SEOUL, "pnu", "*,param.the_geom",
								JoinOptions.INNER_JOIN)
					.project("the_geom, 고유번호 as pnu, 용도지역지구코드 as code, 용도지역지구명 as name")
					.store(RESULT)
					.build();
		result = marmot.createDataSet(RESULT, plan, StoreDataSetOptions.FORCE);
		watch.stop();

		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
	
	private static Geometry getSeoulBoundary(MarmotRuntime marmot) {
		Plan plan;
		
		DataSet sid = marmot.getDataSet(SID);
		plan = marmot.planBuilder("get_seoul")
					.load(SID)
					.filter("ctprvn_cd == '11'")
					.build();
		return marmot.executeLocally(plan).toList().get(0)
										.getGeometry(sid.getGeometryColumnInfo().name());
	}
	
	private static void getSeoulCadastral(MarmotRuntime marmot, Geometry seoul, String output) {
		Plan plan;
		
		DataSet taxi = marmot.getDataSet(CADASTRAL);
		
		plan = marmot.planBuilder("grid_taxi_logs")
					// 택시 로그를  읽는다.
					.query(CADASTRAL, seoul)
					// 승하차 로그만 선택한다.
					.filter("pnu.startsWith('11')")
					.store(output)
					.build();
		GeometryColumnInfo gcInfo = taxi.getGeometryColumnInfo();
		marmot.createDataSet(output, plan, FORCE(gcInfo));
	}
}
