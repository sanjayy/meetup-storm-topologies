/**
 * 
 */
package metascale.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.Map;

public class RedisBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private Jedis jedis;
    private OutputCollector outputCollector;

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map,
	 * backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        jedis = new Jedis("localhost");
        this.outputCollector = collector;
    }

	/*
	 * (non-Javadoc)
	 * 
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
        JSONObject rsvp = (JSONObject) input.getValueByField("rsvp");
        Object country = ((Map) rsvp.get("group")).get("group_country");
        System.out.println("country : " + country);
        jedis.hincrBy("meetup_country", country.toString(), 1);
        Collection topics = ((Collection)((Map) rsvp.get("group")).get("group_topics"));
        for(Object topics_map:topics)
        {
            //System.out.println(((Map)topics_map).get("url_key"));
            jedis.hincrBy("topics", ((Map)topics_map).get("urlkey").toString(), 1);
            jedis.publish("meetup_topics", ((Map)topics_map).get("topic_name").toString());
        }

	}

    /*
      * (non-Javadoc)
      *
      * @see
      * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.
      * topology.OutputFieldsDeclarer)
      */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
