package schema;

import java.util.Arrays;
import java.util.Properties;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import utils.Utils;

public class Create {

	private static String KEYSPACE = "University";	
	private static String COLUMN_FAMILY = "StudentPhotos";	
	private static String SUPER_COLUMN_FAMILY = "ProfessorPublications";
	
	private static String HOST_NAME = "MyHost";
	private static String RPC_PORT = "9160";
	private static String ADDRESS = "localhost";
	private static String IS_SUPER = "false";
	
	private static Properties prop;
		
	private static String CDC = utils.Utils.CDC;
	
	private static boolean VERBOSE = false;
	
	public static void createStandardSchema(Cluster cluster) {
		ColumnFamilyDefinition standardCfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, COLUMN_FAMILY, ComparatorType.UTF8TYPE);
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(standardCfDef));
		cluster.addKeyspace(newKeyspace, true);		
	}
	
	public static void createSuperSchema(Cluster cluster) {
		ColumnFamilyDefinition standardCfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, COLUMN_FAMILY, ComparatorType.UTF8TYPE);
		ColumnFamilyDefinition superCfDef = HFactory.createColumnFamilyDefinition(KEYSPACE, SUPER_COLUMN_FAMILY, ComparatorType.UTF8TYPE);
		superCfDef.setColumnType(ColumnType.SUPER);
		KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(KEYSPACE, ThriftKsDef.DEF_STRATEGY_CLASS, 1, Arrays.asList(superCfDef, standardCfDef));
		cluster.addKeyspace(newKeyspace, true);		
	}
	
	public static void createKeyspaceStdColumnFamily() {
		Cluster cluster = HFactory.getOrCreateCluster(HOST_NAME, ADDRESS + ":" + RPC_PORT);
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);
		
		if (keyspaceDef == null) {
			createStandardSchema(cluster);
		}
	}
	
	public static void createKeyspaceSuperColumnFamily() {
		Cluster cluster = HFactory.getOrCreateCluster(HOST_NAME, ADDRESS + ":" + RPC_PORT);
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);
		
		if (keyspaceDef == null) {
			createSuperSchema(cluster);
		}
	}
	
	public static void dropKeyspace() {
		Cluster cluster = HFactory.getOrCreateCluster(HOST_NAME, ADDRESS + ":" + RPC_PORT);
		cluster.dropKeyspace(KEYSPACE);
	}
	
	public static void main(String... args) throws Exception {
		String path = args.length > 0 ? args[0] : "./CDC.properties";
		
		VERBOSE = args.length > 1 && args[1].equals("-verbose") ? true : false;
		
    	//System.out.println(CDC + "Properties loaded from: " + path);
    	
    	prop = Utils.getCassandraProp(path);
					
		KEYSPACE = prop.getProperty("cassandra.keyspace");
		COLUMN_FAMILY = prop.getProperty("cassandra.column_family");
    	IS_SUPER = prop.getProperty("cassandra.is_super_cf");		
		
    	if (IS_SUPER.equals("true")) {
    		
    		try {
	    		// Droping an unexisting keyspace will cause exception
				dropKeyspace();
				createKeyspaceStdColumnFamily();
				
				System.out.println(CDC + "Previous keyspace droped. Fresh one created.");

			} catch (Exception e) {			
				createKeyspaceStdColumnFamily();
			}		
    	}
    	else {
    		
    		try {
	    		// Droping an unexisting keyspace will cause exception
				dropKeyspace();
				createKeyspaceSuperColumnFamily();
				
				System.out.println(CDC + "Previous keyspace droped. Fresh one created.");

			} catch (Exception e) {			
				createKeyspaceStdColumnFamily();
			}	
    	}
    	
    	if (VERBOSE)
    		System.out.println(CDC + "Schema created!");
	}
}
