package metascale;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import metascale.bolt.RedisBolt;
import metascale.spout.MeetupRsvpsSpout;

/**
 * Created with IntelliJ IDEA.
 * User: syermalk
 * Date: 10/9/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class Topology {
    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // create definition of main spout for queue 1
        topologyBuilder.setSpout("meetup_rsvp_spout", new MeetupRsvpsSpout());
        topologyBuilder.setBolt("redis_bolt", new RedisBolt()).shuffleGrouping("meetup_rsvp_spout");
        Config conf = new Config();
        conf.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("meetup_topology", conf, topologyBuilder.createTopology());
        }
    }

}
