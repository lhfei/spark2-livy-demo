package cn.lhfei.spark2.livy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiJobDriver {
	private static final Logger LOGGER = LoggerFactory.getLogger(PiJobDriver.class);
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		if (args.length < 2) {
			System.out.print("\r\n===== Usage: --------\r\n    PiJobDriver <Livy server url> <slices>");

			System.exit(-1);
		}
		
		Properties props = new Properties();
		props.put("livy.impersonation.enabled", "true");
		props.put("livy.server.auth.kerberos.keytab", "/etc/security/keytabs/spnego.service.keytab");
		props.put("livy.server.auth.kerberos.principal", "HTTP/_HOST@POLARIS.JD.COM");
		props.put("livy.server.auth.type", "kerberos");
		props.put("livy.server.csrf_protection.enabled", "true");
		props.put("livy.server.kerberos.keytab", "/etc/security/keytabs/livy.service.keytab");
		props.put("livy.server.kerberos.principal", "livy/_HOST@POLARIS.JD.COM");
		props.put("livy.server.port", "8999");
		props.put("livy.server.session.timeout", "3600000");
		props.put("livy.superusers", "zeppelin-pss_cloud_dev");
		

		LivyClient client = new LivyClientBuilder().setAll(props).setURI(new URI(args[0])).build();
		
		LOGGER.info("Uploading spark2-livy-demo jar to the SparkContext ...");
		try {
			for(String s : System.getProperty("java.class.path").split(File.pathSeparator)) {
				LOGGER.info("jar file: {}", s);
				if(new File(s).getName().startsWith("spark2-livy-demo")) {
					client.uploadJar(new File(s)).get();
					
					break;
				}
			}
			
			final Integer slices = Integer.parseInt(args[1]);
			double pi = client.submit(new PiJob(slices)).get();
			
			LOGGER.info("Pi is roughly {}", pi);
			
		} catch (InterruptedException e) {
			LOGGER.error("Job start failed. {}", e.getMessage(), e);
		} catch (ExecutionException e) {
			LOGGER.error("Job execute failed. {}", e.getMessage(), e);
		} finally {
			client.stop(true);
		}
	}

}
