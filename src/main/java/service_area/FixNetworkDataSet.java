package service_area;

import static marmot.optor.StoreDataSetOptions.FORCE;

import org.apache.log4j.PropertyConfigurator;

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
public class FixNetworkDataSet {
	private static final String INPUT = "교통/도로/네트워크_추진단";
	private static final String RESULT = "교통/도로/네트워크_fixed";
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
		
		CommandLineParser parser = new CommandLineParser("fix_network_dataset ");
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
		
		DataSet input = marmot.getDataSet(INPUT);
		String geomCol = input.getGeometryColumn();
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		String updEXpr = String.format("%1$s=id.startsWith('D') ? %1$s.reverse() : %1$s", geomCol);
		
		Plan plan;
		plan = Plan.builder("fix_network_dataset")
					.load(INPUT)
					.update(updEXpr)
					.store(RESULT, FORCE(gcInfo))
					.build();
		marmot.execute(plan);
		DataSet ds = marmot.getDataSet(RESULT);
		ds.cluster();

		System.out.printf("elapsed=%s%n", watch.getElapsedMillisString());
		marmot.close();
	}
}
