package oz.hadoop.yarn.api;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import oz.hadoop.yarn.api.utils.PrintUtils;

public class PrettyMapTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
System.out.println(InetAddress.getLocalHost());
//		System.out.println(PrintUtils.prettyMap(System.getenv()));
//		System.out.println(PrintUtils.prettyMap(System.getProperties()));
		
//		InetAddress[] iAddress = InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
//        for (InetAddress inetAddress : iAddress) {
//        	System.out.println(inetAddress.getHostAddress());
//		}
        
//		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
//		while (interfaces.hasMoreElements()) {
//	        NetworkInterface nic = interfaces.nextElement();
//	        Enumeration<InetAddress> addresses = nic.getInetAddresses();
//	        while (addresses.hasMoreElements()) {
//	            InetAddress address = addresses.nextElement();
//	            if (!address.isLoopbackAddress()) {
//	                System.out.println(address.getHostName());
//	            }
//	        }
//	    }
//		Map<String, Object> map = new HashMap<String, Object>();
//		map.put("fname", "Oleg");
//		map.put("lname", "Zhurakousky");
//		map.put("nicknames", Arrays.asList(new String[]{"dodik", "zhorka"}));
//		
//		Map<String, Object> map2 = new HashMap<String, Object>();
//		map2.put("fname", "Seva");
//		map2.put("lname", "Tysh");
//		
//		map.put("children", map2);
//		
//		
//		Map<String, Object> map3 = new HashMap<String, Object>();
//		map3.put("schools", Arrays.asList(new String[]{"Ft. Washington Elementary", "Gov Miflin"}));
//		map3.put("foo", "bar");
//		
//		Map<String, Object> map4 = new HashMap<String, Object>();
//		map4.put("abc", "xyz");
//		
//		map3.put("blahblah", Arrays.asList(new Object[]{"Blah", map4}));
//		
//		map2.put("schools", map3);
//		
//		String prettyMap = PrintUtils.prettyMap(map);
//		System.out.println(prettyMap);
	}

}
