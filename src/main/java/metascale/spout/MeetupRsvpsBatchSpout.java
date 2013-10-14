package metascale.spout;


import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

public class MeetupRsvpsBatchSpout implements IBatchSpout {

    static String STREAMING_API_URL="http://stream.meetup.com/2/rsvps";

    private DefaultHttpClient client;

    static Logger LOG = Logger.getLogger(MeetupRsvpsBatchSpout.class);
    static JSONParser jsonParser = new JSONParser();


    @Override
    public void open(Map map, TopologyContext topologyContext) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void emitBatch(long l, TridentCollector collector) {
        /*
           * Create the client call
           */
        client = new DefaultHttpClient();
        HttpGet get = new HttpGet(STREAMING_API_URL);
        HttpResponse response;
        try {
            //Execute
            response = client.execute(get);
            StatusLine status = response.getStatusLine();
            if(status.getStatusCode() == 200){
                InputStream inputStream = response.getEntity().getContent();
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String in;
                //Read line by line
                int count = 0;
                while((in = reader.readLine())!=null && ++count < 10){
                    try{
                        //Parse and emit
                        //System.out.println(in);
                        Object json = jsonParser.parse(in);
                        collector.emit(Arrays.asList(new Object[]{json}));
                    }catch (Exception e) {
                        LOG.error("Error parsing message from meetup",e);
                    }
                }

            }
        } catch (IOException e) {
            LOG.error("Error in communication with meetup api ["+get.getURI().toString()+"]");
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e1) {
            }
        }
    }

    @Override
    public void ack(long l) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map getComponentConfiguration() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("rsvp");
    }
}
