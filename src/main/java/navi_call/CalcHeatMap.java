package navi_call;

import static marmot.StoreDataSetOptions.*;
import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.COUNT;

import org.apache.log4j.PropertyConfigurator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.command.MarmotClientCommands;
import marmot.geo.GeoClientUtils;
import marmot.optor.geo.SquareGrid;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.Size2d;
import utils.Size2i;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CalcHeatMap {
	private static final String TAXI_LOG = "로그/나비콜/택시로그";
	private static final String SEOUL = "시연/서울특별시";
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
		
		StopWatch watch = StopWatch.start();
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet border = marmot.getDataSet(SEOUL);
		String srid = border.getGeometryColumnInfo().srid();
		Envelope envl = border.getBounds();
		Polygon key = GeoClientUtils.toPolygon(envl);

		Size2i resol = new Size2i(50, 50);
		Size2d cellSize = GeoClientUtils.divide(envl, resol);
		
		Plan plan = marmot.planBuilder("calc_heat_map")
							.loadGrid(new SquareGrid(envl, cellSize), 32)
							.spatialJoin("the_geom", TAXI_LOG, "*")
							.aggregateByGroup(Group.ofKeys("cell_id").tags("the_geom"), COUNT())
							.store(RESULT)
							.build();
		GeometryColumnInfo gcInfo = new GeometryColumnInfo("the_geom", srid);
		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		
		SampleUtils.printPrefix(result, 5);
	}
}
