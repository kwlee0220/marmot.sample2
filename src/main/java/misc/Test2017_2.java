package misc;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;

import common.SampleUtils;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.geo.SquareGrid;
import marmot.plan.GeomOpOptions;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.Size2d;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Test2017_2 {
	private static final String ADDR_BLD = "건물/위치";
	private static final String ADDR_BLD_UTILS = "tmp/test2017/buildings_utils";
	private static final String GRID = "tmp/test2017/grid30";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");

		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = MarmotClientCommands.connect();
		
		StopWatch watch = StopWatch.start();

		DataSet input = marmot.getDataSet(ADDR_BLD);
		String srid = input.getGeometryColumnInfo().srid();
		Envelope bounds = input.getBounds();
		Size2d cellSize = new Size2d(30, 30);
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		
		Plan plan = marmot.planBuilder("get_biz_grid")
								.load(ADDR_BLD_UTILS)
								.buffer("the_geom", 100, GeomOpOptions.OUTPUT("buffer"))
								.assignGridCell("buffer", new SquareGrid(bounds, cellSize), false)
								.centroid("cell_geom")
								.intersectsBinary("cell_geom", "the_geom")
								.aggregateByGroup(Group.ofKeys("cell_id").tags("cell_geom"), COUNT())
								.project("cell_geom as the_geom,*-{cell_geom}")
								.store(GRID, FORCE(gcInfo))
								.build();
		marmot.execute(plan);
		DataSet result = marmot.getDataSet(GRID);
		watch.stop();
		
		SampleUtils.printPrefix(result, 5);
		System.out.println("elapsed: " + watch.getElapsedMillisString());
	}
}
