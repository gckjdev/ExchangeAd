package com.orange.game.api.server;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alipay.client.trade.Trade;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.orange.common.mongodb.MongoDBClient;
import com.orange.game.constants.DBConstants;
import com.orange.game.model.service.CreateDataFileService;
import com.orange.game.model.service.DBService;


public class ExchangeAdServer extends AbstractHandler {

	
	public static final Logger log = Logger.getLogger(ExchangeAdServer.class.getName());		
	
	public static final String VERSION_STRING = ExchangeAdServer.class.getName() + " Version 1.0 Beta";
	public static final String SPRING_CONTEXT_FILE = "classpath:/com/orange/game/api/server/applicationContext.xml";	
	public static final String LOG4J_FLE = "classpath:/log4j.properties";
//	public static final String MONGO_SERVER = "localhost";
//	public static final String MONGO_DB_NAME = "groupbuy";
//	public static final String MONGO_USER = "";
//	public static final String MONGO_PASSWORD = "";
	
	private static final MongoDBClient mongoClient = DBService.getInstance().getMongoDBClient();
	private static final ExecutorService adHistoryService = Executors.newSingleThreadExecutor();
	private static final MongoDBClient mongoClientForPool = new MongoDBClient(DBConstants.D_GAME);	

	public String getAppNameVersion() {
		return VERSION_STRING;
	}

	public String getLog4jFile() {
		return LOG4J_FLE;
	}

	public String getSpringContextFile() {
		return SPRING_CONTEXT_FILE;
	}
	
	public int getPort() {
		String port = System.getProperty("server.port");
		if (port != null && !port.isEmpty()){
			return Integer.parseInt(port);
		}
		return 9879;
	}
	// not used so far
	private void readConfig(String filename){

		   InputStream inputStream = null;		   
		   try {
			   inputStream = new FileInputStream(filename);
		   } catch (FileNotFoundException e) {
			   log.info("configuration file "+filename+"not found exception");
			   e.printStackTrace();
		   }
		   Properties p = new Properties();   
		   try {   
			   p.load(inputStream);   
		   } catch (IOException e1) {   
			   log.info("read configuration file exception");
			   e1.printStackTrace();   
		   }   		   		   
	}
		
	public static void initSpringContext(String... context) {
		try {
			new ClassPathXmlApplicationContext(
					context );
		} catch (Exception e) {
			log.info("initSpringContext exception");
			e.printStackTrace();
		}
	}
	
	static boolean isTrade = true;
	
    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) 
        throws IOException, ServletException
    {
    	
    	
        baseRequest.setHandled(true);
        Trade trade = new Trade();
        if (isTrade){
        	//trade.doPost(request, response, "", "", "", "", "");
        	return;
        }
        
        
		try{			
			
			log.info("RECV request="+request.toString());
			
			final String appName = request.getParameter("app");
			final String source = request.getParameter("source");
			
			String appURL = DBConstants.C_DEFAULT_APP_URL;
			if (appName != null && appName.length() > 0){
					BasicDBObject query = new BasicDBObject(DBConstants.F_APP_NAME, appName);
					DBObject obj = mongoClient.findOne(DBConstants.T_EXCHANGE_AD, query);
					if (obj != null){
						appURL = (String)obj.get(DBConstants.F_APP_URL);
					}
			}
			
			if (appURL == null || appURL.length() == 0){
				log.info("warning, app url is null or empty");
				appURL = DBConstants.C_DEFAULT_APP_URL;
			}
			
			log.info("REDIRECT to "+appURL);
//			response.setStatus(HttpServletResponse.SC_MOVED_PERMANENTLY);
//			response.setHeader("Location", appURL);
			response.sendRedirect(appURL);      
			response.flushBuffer();

			// write down history
			final String url = appURL;
			final String remoteInfo = request.getRemoteAddr()+","+request.getRemotePort();
			adHistoryService.execute(new Runnable() {
				
				@Override
				public void run() {
					
					// insert one record
					BasicDBObject obj = new BasicDBObject();
					obj.put(DBConstants.F_SOURCE, source);
					obj.put(DBConstants.F_REDIRECT_APP, appName);
					obj.put(DBConstants.F_REDIRECT_URL, url);
					obj.put(DBConstants.F_CREATE_DATE, new Date());
					obj.put(DBConstants.F_REMOTE_URL, remoteInfo);
					log.info("<insert> obj="+obj.toString());
					mongoClientForPool.insert(DBConstants.T_EXCHANGE_AD_HISTORY, obj);
					
				}
			});
			
		} catch (Exception e){
			log.error("<handleHttpServletRequest> catch Exception="+e.toString(), e);		
		} finally {
		}		
    }

	public void startServer() throws Exception {
    	//init the spring context
		String[] springFiles = new String[] { getSpringContextFile() };
    	initSpringContext(springFiles);    	
    	
		log.info(getAppNameVersion());
    	
		Server server = new Server(getPort());
		server.setHandler(this);
        
        QueuedThreadPool threadPool = new QueuedThreadPool();  
        threadPool.setMaxThreads(100);
        threadPool.setMinThreads(10);
        server.setThreadPool(threadPool);  
        server.setStopAtShutdown(true);
        server.start();
        server.join();
	}
	
	
	

	
	
	
	
	
    public static void main(String[] args) throws Exception{
    	    	
//    	CreateDataFileService.getInstance().execute(mongoClient);
    	
		// This code is to initiate the listener.
		/*ServerMonitor.getInstance().start();
    	
		ExchangeAdServer adServer = new ExchangeAdServer();
		adServer.startServer();*/
    	
        	String para = System.getProperty("exChange.para");
        	CreateDataFileService createDataFileService = CreateDataFileService.getInstance();
        	log.info("PARA = "+para);


        	if (Integer.parseInt(para) == 1) {
        		createDataFileService.hotExecute(mongoClient, DBConstants.C_LANGUAGE_CHINESE);
        		createDataFileService.hotExecute(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
    		}else if (Integer.parseInt(para) == 2) {
    			createDataFileService.allTimeExecute(mongoClient, DBConstants.C_LANGUAGE_CHINESE);
    			createDataFileService.allTimeExecute(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
    		}else if (Integer.parseInt(para) == 3) {
    			createDataFileService.featureExcute(mongoClient, DBConstants.C_LANGUAGE_CHINESE);    	
    			createDataFileService.featureExcute(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
    		}else if (Integer.parseInt(para) == 4) {
    			createDataFileService.contestHotExecute(mongoClient, DBConstants.C_LANGUAGE_CHINESE);
    			createDataFileService.contestHotExecute(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
			}else if (Integer.parseInt(para) == 5) {
				createDataFileService.contestLatestExecute(mongoClient, DBConstants.C_LANGUAGE_CHINESE);
				createDataFileService.contestLatestExecute(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
			}else if (Integer.parseInt(para) == 6) {
				createDataFileService.myContestOpus(mongoClient, DBConstants.C_LANGUAGE_CHINESE);
				createDataFileService.myContestOpus(mongoClient, DBConstants.C_LANGUAGE_ENGLISH);
			}else if (Integer.parseInt(para) ==7) {
				createDataFileService.userFavorite(mongoClient);
            }else if (Integer.parseInt(para) == 8) {
                createDataFileService.moveMessage(mongoClient);
            }else if (Integer.parseInt(para) == 9){
            	log.info("<moveBBSData> Start");
                createDataFileService.moveBBSData(mongoClient);
                log.info("<moveBBSData> Done");
            }    	
    	
    }
}
