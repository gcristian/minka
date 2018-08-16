package io.tilt.minka.sampler;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import io.tilt.minka.api.Client;
import io.tilt.minka.api.Config;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.EventMapper;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.api.Reply;
import io.tilt.minka.api.Server;
import io.tilt.minka.core.leader.balancer.EvenSizeBalancer;
import io.tilt.minka.shard.TCPShardIdentifier;

public class CmdLineApp {

	public static boolean useDefaults = Boolean.parseBoolean(System.getProperty("useDefaults", "true"));
	
	public static void main(String[] args) {
		boolean debug = false;
		if (args!=null && args.length>0) {
			for (String arg: args) {
				debug |= arg.trim().equalsIgnoreCase("debug:true");
			}
		}
		new CmdLineApp().run(debug);
		System.exit(1);
	}

	public enum Quest {
		namespace("cluster Namespace (to reach other nodes)"),
		tag("node Tag (for tagging your node within the cluster)"),
		//address("node address (your public LAN ip)"),
		zk("Zookeeper (ask Cristian)"),
		address("Address (confirm if is a public address IP)")
		;
		private String title;

		Quest(String v) {
			this.title = v;
		}
		public String getTitle() {
			return title;
		}
	}
	
	private void run(final boolean debug) {
		Server server = null;
		try (Scanner scan = new Scanner(System.in)) {
			final Map<Quest, String> quest = readParameters(scan);
			server = createServer(quest, debug);
			readCmdLine(server.getClient(), scan, quest);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (server!=null) {
				server.shutdown();
			}
			System.out.println("good bye !");
		}
	}

	private Map<Quest, String> readParameters(final Scanner scan) {
		
		final Map<Quest, String> suggest = new HashMap<>();
		suggest.put(Quest.namespace, "demo");
		final String ms = String.valueOf(System.currentTimeMillis());
		suggest.put(Quest.tag, System.getProperty("user.name") + "-" + ms.substring(ms.length()-5));
		suggest.put(Quest.zk, "localhost:2181");
		suggest.put(Quest.address, TCPShardIdentifier.findLANAddress().getHostAddress());
		
		final Map<Quest, String> quest = new HashMap<>();
		
		System.out.println("Welcome ! First some answers (always type quit to exit)");
		System.out.println("==========================================================");
		for(Quest q: Quest.values()) {
			while (!Thread.interrupted() && true) {
				System.out.println("Enter " + q.getTitle() + ": ");
				System.out.print("\t( " + suggest.get(q) + " ? ) ");
				if (useDefaults || scan.hasNextLine()) {
					String line = useDefaults ? null : scan.nextLine();
					if ((line==null || line.length()<1)&& suggest.containsKey(q)) {
						quest.put(q, suggest.get(q));
						break;
					} else if (line.equalsIgnoreCase("quit")) {
						break;
					} else {
						quest.put(q, line);
						break;
					}
				} else {
					System.out.println("no input !");
				}
			}
		}
		return quest;
	}
	
	private Server createServer(final Map<Quest, String> quest, final boolean debug) {
		
		final Config ownConfig = new Config();
		
		ownConfig.getBroker().setHostAndPort(quest.get(Quest.address), 5000);
		ownConfig.getBootstrap().setServerTag(quest.get(Quest.tag));
		ownConfig.getBootstrap().setNamespace(quest.get(Quest.namespace));
		ownConfig.getBootstrap().setZookeeperHostPort(quest.get(Quest.zk));
		
		ownConfig.getBroker().setEnablePortFallback(true);
		ownConfig.getBootstrap().setBeatUnitMs(100);
		ownConfig.getBootstrap().setDropVMLimit(true);
		ownConfig.getBootstrap().setEnableCoreDump(debug);
		ownConfig.getBootstrap().setCoreDumpFilepath("/tmp/");

		final Set<Duty> everCaptured = new HashSet<>();
		final Set<Duty> everReleased = new HashSet<>();
		final Set<Duty> current = new HashSet<>();
		final Object[] o = {null};
		final Server server = new Server(ownConfig);
		final EventMapper mapper = server.getEventMapper();
		
		mapper.onPalletLoad(() -> Collections.emptySet())
			.onActivation(()->{})
			.onDeactivation(()->{})
			.onTransfer((a,b)->{})
			.onLoad(()-> Collections.emptySet())
			.onPalletRelease(p->o[0]=p)
			.onPalletCapture(p->o[0]=p)
			.onCapture(d-> { 
				everCaptured.addAll(d);
				current.addAll(d);
			})
			.onRelease(d-> {
				everReleased.addAll(d);
				current.removeAll(d);
			})
			.done();
		
		return server;
	}
	
	private void readCmdLine(final Client  client, final Scanner scan, final Map<Quest, String> quest) {
		System.out.println("Now enter tasks");
		final String help = "Syntaxis: \n"
				+ "\n\t d {pallet-id} {duty-id} {weight}   creates a duty (! prefixing 'd' will delete it)"
				+ "\n\t p {pallet-id} {balancer}           creates a pallet (balancers: even_size, fair_weight, even_weight) "
				+ "\n\t c {pallet-id} {weight]             reports capacity for a pallet"
				+ "\n\t quit                               terminates session \n"
				+ "\nSample: p PALLET1 (creates a pallet)"
				+ "\nSample: d PALLET1 DUTY1 99 (creates a duty inside with weight)"
				+ "\nSample: !d PALLET1 DUTY1 (removes previous duty"
				;
		
		System.out.println(help);
		while (scan.hasNextLine() && !Thread.interrupted()) {
			String cmd = scan.nextLine();
			try {
				if (cmd==null || cmd.length()<1) {
					System.out.println(help);
					System.out.println("type <quit> to terminate ");
					continue;
				} else if (cmd.equalsIgnoreCase("quit")) {
					break;
				}
				
				final String[] split = cmd.split(" ");
				
				boolean run = false;
				if (split.length==1 || split.length>4) {
				} else {
					run = run(split, client);
				}
				if (!run) {
					System.out.println(help);
					System.out.println("Unknown command or format !");
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		
		}
	}
	
	private boolean run(String[] split, final Client client) {
		String cmd=split[0].toLowerCase();
		final String tagPrefix = ""; //quest.get(Quest.tag) + "-";
		Reply res = null;
		if (cmd.equals("d")) {
			res = client.add(duty(tagPrefix, split));
		} else if (cmd.equals("!d")) {
			res = client.remove(duty(tagPrefix, split));
		} else if (cmd.equals("p")) {
			res = client.add(pallet(split));
		} else if (cmd.equals("!p")) {
			res = client.remove(pallet(split));
		} else if (cmd.equals("c")) {
			client.getEventMapper().setCapacity(pallet(split), 999d);
		}
		if (res!=null) {
			System.out.println(res.toMessage());
			return true;
		} else {
			return false;
		}
	}
	
	public Duty duty(final String tagPrefix, String[] s) {
		final String id = StringUtils.remove(StringUtils.abbreviate(s[2], 32).trim(), " ");
		final DutyBuilder bldr = Duty.builder(tagPrefix + id, s[1]);
		if (s.length==4) {
			bldr.with(Double.valueOf(s[3]));
		}
		return bldr.build();
	}
	
	public Pallet pallet(String[] s) {
		final String id = StringUtils.remove(StringUtils.abbreviate(s[1], 32).trim(), " ");
		final PalletBuilder bldr = Pallet.builder(id);
		if (s.length==3) {
			bldr.with(new EvenSizeBalancer.Metadata());
		}
		return bldr.build();
	}
	
}
