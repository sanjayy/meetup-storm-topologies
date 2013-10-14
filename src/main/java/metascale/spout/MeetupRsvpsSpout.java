package metascale.spout;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class MeetupRsvpsSpout extends BaseRichSpout{

    static String STREAMING_API_URL="http://stream.meetup.com/2/rsvps";

    private DefaultHttpClient client;
    private SpoutOutputCollector collector;

    static Logger LOG = Logger.getLogger(MeetupRsvpsSpout.class);
    static JSONParser jsonParser = new JSONParser();

    @Override
    public void nextTuple() {
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
                while((in = reader.readLine())!=null){
                    try{
                        //Parse and emit
                        //System.out.println(in);
                        Object json = jsonParser.parse(in);
                        collector.emit(new Values(json));
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
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rsvp"));
    }



}
