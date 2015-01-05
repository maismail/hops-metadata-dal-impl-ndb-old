/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb;

import se.sics.hop.metadata.ndb.wrapper.HopsSession;

/**
 *
 * @author salman
 */
public class DBSession {
    
    private HopsSession session;
    private final int MAX_REUSE_COUNT;
    private int sessionUseCount;

    public DBSession(HopsSession session, int maxReuseCount) {
        this.session = session;
        this.MAX_REUSE_COUNT = maxReuseCount;
        this.sessionUseCount = 0;
    }

    public HopsSession getSession() {
        return session;
    }

    public int getSessionUseCount() {
        return sessionUseCount;
    }

    public void setSessionUseCount(int sessionUseCount) {
        this.sessionUseCount = sessionUseCount;
    }

    public int getMaxReuseCount() {
        return MAX_REUSE_COUNT;
    }
}
