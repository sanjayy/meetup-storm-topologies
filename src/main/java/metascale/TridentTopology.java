package metascale;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import metascale.spout.MeetupRsvpsBatchSpout;
import metascale.trident.RedisState;
import org.json.simple.JSONObject;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 10/9/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class TridentTopology {
    public static void main(String[] args) {

        StateFactory redis = RedisState.nonTransactional(new InetSocketAddress("localhost", 6379));
        MeetupRsvpsBatchSpout meetup_rsvp_spout = new MeetupRsvpsBatchSpout();
        storm.trident.TridentTopology topology = new storm.trident.TridentTopology();

        topology.newStream("meetup_rsvp_spout", meetup_rsvp_spout).parallelismHint(1)
                .each(new Fields("rsvp"), new Extract(), new Fields("country", "response"))
                .groupBy(new Fields("country", "response"))
                .persistentAggregate(redis, new Count(), new Fields("cnt")).parallelismHint(1);

        Config conf = new Config();
        conf.setMaxSpoutPending(20);

        conf.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, topology.build());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("meetup_trident_topology", conf, topology.build());
        }
    }

    private static class Extract extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector tridentCollector) {
            JSONObject rsvp = (JSONObject) tuple.get(0);

            Object country = ((Map) rsvp.get("group")).get("group_country");
            Object response = rsvp.get("response");
            System.out.println(" in extract: " + country + " : " + response);
            tridentCollector.emit(new Values(country.toString(), response.toString()));
        }


    }
}
