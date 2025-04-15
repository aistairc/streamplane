package jp.go.aist.streamplane;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

public class ImdgConfig {

    public static IgniteConfiguration CONFIG(){
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

//        spi.setLocalAddress("localhost");
//        spi.setLocalPort(57500);

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        // Set initial IP addresses.
        // Note that you can optionally specify a port or a port range.
//        ipFinder.setAddresses(Arrays.asList("localhost:47500"));
        ipFinder.setAddresses(Arrays.asList("ignite"));
//        ipFinder.setAddresses(Arrays.asList("10.0.1.10", "10.0.1.201","10.0.1.202","10.0.1.203","10.0.1.204","10.0.1.205","10.0.1.101", "10.0.1.102", "10.0.1.103", "10.0.1.104", "10.0.1.105"));
        spi.setIpFinder(ipFinder);

//        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
//        commSpi.setLocalPort(57100);


        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setClientMode(true);
        igniteCfg.setDiscoverySpi(spi);
//        igniteCfg.setCommunicationSpi(commSpi);

        return igniteCfg;
    }

}
