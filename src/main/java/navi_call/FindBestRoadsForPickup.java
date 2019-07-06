package navi_call;

import static marmot.StoreDataSetOptions.FORCE;
import static marmot.optor.AggregateFunction.COUNT;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.log4j.PropertyConfigurator;

import common.SampleUtils;
import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.RecordSet;
import marmot.command.MarmotClientCommands;
import marmot.externio.shp.ExportRecordSetAsShapefile;
import marmot.externio.shp.ShapefileParameters;
import marmot.plan.Group;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FindBestRoadsForPickup {
	private static final String INPUT = Globals.TAXI_LOG_MAP;
	private static final String RESULT = "tmp/result";
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
		
		DataSet ds = marmot.getDataSet(INPUT);
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		
		Plan plan;
		plan = marmot.planBuilder("match_and_rank_roads")
					.load(INPUT)
					.filter("status == 0")
					.defineColumn("hour:int", "ts.substring(8,10)")
					.aggregateByGroup(Group.ofKeys("hour,link_id,sub_link_no")
											.withTags("link_geom"), COUNT())
					.project("link_geom as the_geom,*-{link_geom}")
					.filter("count >= 50")
					.listByGroup(Group.ofKeys("hour").tags("the_geom").orderBy("count:D"))
					.store(RESULT)
					.build();

		DataSet result = marmot.createDataSet(RESULT, plan, FORCE(gcInfo));
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
		
		SampleUtils.printPrefix(result, 10);
	}
	
	private static void exportResult(PBMarmotClient marmot, String dsId,
									String baseDirPath) throws IOException, InterruptedException {
		export(marmot, dsId, 8, baseDirPath);
		export(marmot, dsId, 22, baseDirPath);
	}
	
	private static void export(PBMarmotClient marmot, String dsId, int hour,
								String baseName) throws IOException, InterruptedException {
		Plan plan = marmot.planBuilder("export")
								.load(dsId)
								.filter("hour == " + hour)
								.build();
		RecordSet rset = marmot.executeLocally(plan);

		String file = String.format("/home/kwlee/tmp/%s_%02d", baseName, hour);
		ShapefileParameters params = ShapefileParameters.create()
														.typeName("best_roads")
														.shpSrid(SRID)
														.charset(Charset.forName("euc-kr"));
		ExportRecordSetAsShapefile export = new ExportRecordSetAsShapefile(rset, SRID,
																			file, params);
		export.start().waitForDone();
	}
}
