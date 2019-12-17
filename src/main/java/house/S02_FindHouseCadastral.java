package house;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.remote.protobuf.PBMarmotClient;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S02_FindHouseCadastral {
	private static final String CADASTRAL = "구역/연속지적도_추진단";
//	private static final String CADASTRAL = "tmp/house/cadastral_27";
	private static final String HOUSE_AREA = "tmp/house/house_area";
	private static final String RESULT = "tmp/house/house_cadastral";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();
		
		// 전국 지적도에서 주거지적 추출
		process(marmot, CADASTRAL, HOUSE_AREA, RESULT);
		
		marmot.close();
	}

//	static final DataSet process(MarmotRuntime marmot, String cadastral,
//									String houseAreaId, String result)
//		throws Exception {
//		StopWatch elapsed = StopWatch.start();
//		
//		DataSet left = marmot.getDataSet(cadastral);
//		String leftGeomCol = left.getGeometryColumn();
//		String leftSrid = left.getSRID();
//		
//		DataSet right = marmot.getDataSet(houseAreaId);
//		String rightGeomCol = right.getGeometryColumn();
//		
//		String tempCol = "temp_geom";
//		String joinOutColsExpr = String.format("*,param.%s as %s", rightGeomCol, tempCol);
//		String projectColsExpr = String.format("*-{%s}", tempCol);
//
//		Plan plan = Plan.builder("전국 지적도에서 주거지적 추출")
//						.load(cadastral)
//						.spatialJoin(leftGeomCol, houseAreaId, INTERSECTS, joinOutColsExpr)
//						.shard(45)
//						.intersection("the_geom", tempCol, "the_geom")
//						.dropEmptyGeometry("the_geom")
//						.project(projectColsExpr)
//						.store(result)
//						.build();
//		DataSet ds = marmot.newDataSetBuilder()
//							.setDefaultGeometryColumn(leftGeomCol, leftSrid)
//							.setDataFillingPlan(plan)
//							.setForce(true)
//							.build();
//
//		elapsed.stop();
//		System.out.printf("전국 지적도에서 주거지적 추출 완료, count=%d elapsed=%s%n",
//							ds.getRecordCount(), elapsed.getElapsedTimeString());
//		
//		return ds;
//	}

	static final DataSet process(MarmotRuntime marmot, String cadastral,
									String houseAreaId, String result)
		throws Exception {
		StopWatch elapsed = StopWatch.start();
		
		DataSet left = marmot.getDataSet(cadastral);
		DataSet right = marmot.getDataSet(houseAreaId);
		String rightGeomCol = right.getGeometryColumn();
		
		String tempCol = "temp_geom";
		String joinOutColsExpr = String.format("left.*,right.%s as %s", rightGeomCol, tempCol);
		String projectColsExpr = String.format("*-{%s}", tempCol);
		GeometryColumnInfo gcInfo = left.getGeometryColumnInfo();

		Plan plan = Plan.builder("전국 지적도에서 주거지적 추출")
						.loadSpatialIndexJoin(cadastral, houseAreaId, joinOutColsExpr)
						.shard(45)
						.intersection("the_geom", tempCol, "the_geom")
						.dropEmptyGeometry("the_geom")
						.project(projectColsExpr)
						.store(result, FORCE(gcInfo))
						.build();
		marmot.execute(plan);
		
		DataSet ds = marmot.getDataSet(result);

		elapsed.stop();
		System.out.printf("전국 지적도에서 주거지적 추출 완료, count=%d elapsed=%s%n",
							ds.getRecordCount(), elapsed.getElapsedMillisString());
		
		return ds;
	}
}
