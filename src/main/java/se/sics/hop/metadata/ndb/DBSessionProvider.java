/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.ndb.wrapper.HopsExceptionHelper;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.ndb.wrapper.HopsSessionFactory;

import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author salman
 */
public class DBSessionProvider implements Runnable {

    static final Log LOG = LogFactory.getLog(DBSessionProvider.class);
    static HopsSessionFactory sessionFactory;
    private ConcurrentLinkedQueue<DBSession> sessionPool = new ConcurrentLinkedQueue<DBSession>();
    private ConcurrentLinkedQueue<DBSession> toGC = new ConcurrentLinkedQueue<DBSession>();
    private final int MAX_REUSE_COUNT;
    private Properties conf;
    private final Random rand;
    private AtomicInteger sessionsCreated = new AtomicInteger(0);
    private long rollingAvg[];
    private AtomicInteger rollingAvgIndex = new AtomicInteger(-1);
    private boolean automaticRefresh = false;
    private Thread thread;
    private int initialPoolSize;
    private boolean threadStopped;

    public DBSessionProvider(Properties conf, int reuseCount, int initialPoolSize) throws StorageException {
        this.conf = conf;
        if (reuseCount <= 0) {
            System.err.println("Invalid value for session reuse count");
            System.exit(-1);
        }
        this.MAX_REUSE_COUNT = reuseCount;
        rand = new Random(System.currentTimeMillis());
        rollingAvg = new long[initialPoolSize];
        this.initialPoolSize = initialPoolSize;
        this.threadStopped = false;
        start();
    }
    
    public HopsSessionFactory getSessionFactory(){
        return sessionFactory;
    }

    private void start() throws StorageException {
        System.out.println("Database connect string: " + conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
        System.out.println("Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
        System.out.println("Max Transactions: " + conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));
        try {
            sessionFactory = new HopsSessionFactory(ClusterJHelper.getSessionFactory(conf));
        } catch (ClusterJException ex) {
            throw HopsExceptionHelper.wrap(ex);
        }

        for (int i = 0; i < initialPoolSize; i++) {
            sessionPool.add(initSession());
        }

        thread = new Thread(this, "Session Pool Refresh Daemon");
        automaticRefresh = true;
        thread.start();
    }

    private DBSession initSession() throws StorageException {
        Long startTime = System.currentTimeMillis();
        HopsSession session = sessionFactory.getSession();
        Long sessionCreationTime = (System.currentTimeMillis() - startTime);
        rollingAvg[rollingAvgIndex.incrementAndGet() % rollingAvg.length] = sessionCreationTime;

        int reuseCount = rand.nextInt(MAX_REUSE_COUNT) + 1;
        DBSession dbSession = new DBSession(session, reuseCount);
        sessionsCreated.incrementAndGet();
        return dbSession;
    }
    
    private void closeSession(DBSession dbSession) throws StorageException {
        Long startTime = System.currentTimeMillis();
        dbSession.getSession().close();
        Long sessionCreationTime = (System.currentTimeMillis() - startTime);
        rollingAvg[rollingAvgIndex.incrementAndGet() % rollingAvg.length] = sessionCreationTime;
    }

    public void stop() throws StorageException {
        LOG.info("Session pool is closing now...");
        automaticRefresh = false;
        while(!threadStopped){
            try{
                Thread.sleep(100);
            }catch(Exception e){
                
            }
        }
        LOG.info("Session pool thread stopped.");
        while (!sessionPool.isEmpty()) {
            DBSession dbsession = sessionPool.remove();
            closeSession(dbsession);
        }
        while(!toGC.isEmpty()){
            DBSession dbsession = toGC.remove();
            closeSession(dbsession);
        }
        LOG.info("All sessions closed.");
        sessionFactory.close();
        sessionFactory = null;
        sessionPool.clear();
        toGC.clear();
        LOG.info("Session factory closed.");
        
    }

    public DBSession getSession() throws StorageException {
        try {
            DBSession session = sessionPool.remove();
            return session;
        } catch (NoSuchElementException e) {
            LOG.warn("DB Sessino provider cant keep up with the demand for new sessions");
            return initSession();
        }
    }

    public void returnSession(DBSession returnedSession, boolean forceClose) {
        //session has been used, increment the use counter
        returnedSession.setSessionUseCount(returnedSession.getSessionUseCount() + 1);

        if ((returnedSession.getSessionUseCount() >= returnedSession.getMaxReuseCount())
                || forceClose) { // session can be closed even before the reuse count has expired. Close the session incase of database errors.
            toGC.add(returnedSession);
        } else { // increment the count and return it to the pool
            sessionPool.add(returnedSession);
        }
        
        sessionFactory.releaseResourcesForThisThread();
    }

    public double getSessionCreationRollingAvg() {
        double avg = 0;
        for (int i = 0; i < rollingAvg.length; i++) {
            avg += rollingAvg[i];
        }
        avg = avg / rollingAvg.length;
        return avg;
    }

    public int getTotalSessionsCreated() {
        return sessionsCreated.get();
    }

    public int getAvailableSessions() {
        return sessionPool.size();
    }

    @Override
    public void run() {
        while (automaticRefresh) {
            //System.out.print("+");
            
            try {
                
                int gced = 0;
                int news = 0;

                    while(!toGC.isEmpty()){
                        DBSession session = toGC.remove();
                        session.getSession().close();
                        gced++;
                    }
                    

                    for (int i = 0; i < gced; i++) {
                        sessionPool.add(initSession());
                    }
                    news +=gced;
                    
                    if(getAvailableSessions()< (initialPoolSize/2)){
                        int moreSessions = initialPoolSize/2;
                        for(int i = 0; i < moreSessions; i++){
                            sessionPool.add(initSession());
                        }
                        news += moreSessions;
                    }
                    
//                    if(gced>0 || news > 0){
//                       LOG.debug("CGed " + gced+" New "+ news);
//                   }
                
                Thread.sleep(50);
                
            }  catch (Exception ex) {
                LOG.error(ex);
            }
        }
        threadStopped = true;
    }
}
