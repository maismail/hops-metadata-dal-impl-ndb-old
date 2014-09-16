/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn.fair;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.fair.HopFSSchedulerNode;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import se.sics.hop.metadata.yarn.tabledef.fair.FSSchedulerNodeTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class FSSchedulerNodeClusterJ implements FSSchedulerNodeTableDef, FSSchedulerNodeDataAccess<HopFSSchedulerNode>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface FSSchedulerNodeDTO {

        @PrimaryKey
        @Column(name = RMNODEID)
        String getrmnodeid();
        void setrmnodeid(String rmnodeid);

        @Column(name = NUMCONTAINERS)
        int getnumcontainers();
        void setnumcontainers(int numcontainers);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFSSchedulerNode findById(String id) throws StorageException {
        Session session = connector.obtainSession();

        FSSchedulerNodeClusterJ.FSSchedulerNodeDTO fsschedulernodeDTO = null;
        if (session != null) {
            fsschedulernodeDTO = session.find(FSSchedulerNodeClusterJ.FSSchedulerNodeDTO.class, id);
        }
        if (fsschedulernodeDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFSSchedulerNode(fsschedulernodeDTO);
    }

    @Override
    public void prepare(Collection<HopFSSchedulerNode> modified, Collection<HopFSSchedulerNode> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<FSSchedulerNodeClusterJ.FSSchedulerNodeDTO> toRemove = new ArrayList<FSSchedulerNodeClusterJ.FSSchedulerNodeDTO>();
                for (HopFSSchedulerNode hop : removed) {
                    FSSchedulerNodeClusterJ.FSSchedulerNodeDTO persistable = session.newInstance(FSSchedulerNodeClusterJ.FSSchedulerNodeDTO.class, hop.getRmnodeid());
                    toRemove.add(persistable);
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<FSSchedulerNodeClusterJ.FSSchedulerNodeDTO> toModify = new ArrayList<FSSchedulerNodeClusterJ.FSSchedulerNodeDTO>();
                for (HopFSSchedulerNode hop : modified) {
                    FSSchedulerNodeClusterJ.FSSchedulerNodeDTO persistable = createPersistable(hop, session);
                    toModify.add(persistable);
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFSSchedulerNode(HopFSSchedulerNode node) throws StorageException {
        Session session = connector.obtainSession();
        session.savePersistent(createPersistable(node, session));
    }
    
    private HopFSSchedulerNode createHopFSSchedulerNode(FSSchedulerNodeDTO fsschedulernodeDTO) {
        HopFSSchedulerNode hop = new HopFSSchedulerNode(fsschedulernodeDTO.getrmnodeid(), fsschedulernodeDTO.getnumcontainers());
        
        return hop;
    }
    
    private FSSchedulerNodeDTO createPersistable(HopFSSchedulerNode hop, Session session) {
        FSSchedulerNodeDTO fssDTO = session.newInstance(FSSchedulerNodeDTO.class);
        fssDTO.setrmnodeid(hop.getRmnodeid());
        fssDTO.setnumcontainers(hop.getNumcontainers());
        
        return fssDTO;
    }
    
}
