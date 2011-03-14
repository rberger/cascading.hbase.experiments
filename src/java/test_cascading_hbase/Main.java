package test_cascading_hbase;

import java.io.IOException;
import java.util.Properties;

import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import cascading.tap.SinkMode;
import cascading.hbase.HBaseTap;
import cascading.hbase.HBaseScheme;


public class Main {
	

	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
            String inputFileLhs = args[0];
            // set the current job jar
	    Properties properties = new Properties();
	    FlowConnector.setApplicationJarClass( properties, Main.class );
	    
            /**
             * From the very incomplete example at http://wiki.apache.org/hadoop/Hbase/Cascading
             */
            
            // read data from the default filesystem
            // emits two fields: "offset" and "line"
            Tap source = new Hfs( new TextLine(), inputFileLhs );

            // store data in a HBase cluster
            // accepts fields "num", "lower", and "upper"
            // will automatically scope incoming fields to their proper familyname, "left" or "right"
            Fields keyFields = new Fields( "num" );
            String[] familyNames = {"left", "right"};
            Fields[] valueFields = new Fields[] {new Fields( "lower" ), new Fields( "upper" ) };
            Tap hBaseTap = new HBaseTap( "multitable", new HBaseScheme( keyFields, familyNames, valueFields ), SinkMode.REPLACE );

            // a simple pipe assembly to parse the input into fields
            // a real app would likely chain multiple Pipes together for more complex processing
            Pipe parsePipe = new Each( "insert", new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), " " ) );

            // "plan" a cluster executable Flow
            // this connects the source Tap and hBaseTap (the sink Tap) to the parsePipe
            Flow parseFlow = new FlowConnector( properties ).connect( source, hBaseTap, parsePipe );

            // start the flow, and block until complete
            parseFlow.complete();

            // open an iterator on the HBase table we stuffed data into
            TupleEntryIterator iterator = parseFlow.openSink();
            
            while(iterator.hasNext())
                {
                    // print out each tuple from HBase
                    System.out.println( "iterator.next() = " + iterator.next() );
                }

            iterator.close();
	}
}
