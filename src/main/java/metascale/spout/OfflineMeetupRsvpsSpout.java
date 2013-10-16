package metascale.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 10/14/13
 * Time: 10:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class OfflineMeetupRsvpsSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random _rand;
    String[] sentences = null;
    static JSONParser jsonParser = new JSONParser();


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();

        List list = new ArrayList();
        File file = new File("src/main/resources/meetup_RSVPs.txt");

        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            String line = bufferedReader.readLine();
            System.out.println("line " + line);
            while (line != null  )
            {
                //System.out.println(line);
                list.add(line);
                line = bufferedReader.readLine();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        sentences = new String[list.size()];
        list.toArray(sentences);
        System.out.println(" open : " + list.size());

    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        String message = sentences[_rand.nextInt(sentences.length)];
        try {
            Object jsonObject = jsonParser.parse(message);
            _collector.emit(new Values(jsonObject));

        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rsvp"));
    }

}