package house.misc;

import org.apache.log4j.PropertyConfigurator;

import marmot.DataSet;
import marmot.GeometryColumnInfo;
import marmot.Plan;
import marmot.StoreDataSetOptions;
import marmot.command.MarmotClientCommands;
import marmot.remote.protobuf.PBMarmotClient;
import utils.CommandLine;
import utils.CommandLineParser;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class S01_ExportHouseArea27 {
	private static final String HOUSE_AREA = "tmp/house/house_area";
	
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
		
		// 원격 MarmotServer에 접속.
		PBMarmotClient marmot = PBMarmotClient.connect(host, port);
		
		DataSet input = marmot.getDataSet(HOUSE_AREA);
		
		Plan plan;
		plan = marmot.planBuilder("export01")
					.load(HOUSE_AREA)
					.filter("signgu_se.startsWith('27')")
					.store(HOUSE_AREA + "_27")
					.build();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		DataSet ds = marmot.createDataSet(HOUSE_AREA + "_27", plan, StoreDataSetOptions.create().geometryColumnInfo(gcInfo).force(true));
	}
}
