/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopFifoScheduler;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.FifoSchedulerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.FifoSchedulerTableDef;
import static se.sics.hop.metadata.yarn.tabledef.FifoSchedulerTableDef.QUEUEMETRICS_ID;

/**
 *
 * @author nickstanogias
 */
public class FifoSchedulerClusterJ implements FifoSchedulerTableDef, FifoSchedulerDataAccess<HopFifoScheduler> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FifoSchedulerDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = RMCONTEXT)
        int getrmcontext();
        void setrmcontex(int rmcontext);

        @Column(name = INITIALIZED)
        boolean getinitialized();
        void setinitialized(boolean initialized);

        @Column(name = USEPORTFORNODENAME)
        boolean getuseportfornodename();
        void setuseportfornodename( boolean useportfornodename);

        @Column(name = QUEUEMETRICS_ID)
        int getqueuemetricsid();
        void setqueuemetricsid(int queuemetricsid);
        
        @Column(name = CLUSTERRESOURCE_ID)
        int getclusterresourceid();
        void setclusterresourceid(int clusterresourceid);
        
        @Column(name = USEDRESOURCE_ID)
        int getusedresourceid();
        void setusedresourceid(int usedresourceid);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopFifoScheduler findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        FifoSchedulerClusterJ.FifoSchedulerDTO fifoSchedulerDTO = null;
        if (session != null) {
            fifoSchedulerDTO = session.find(FifoSchedulerClusterJ.FifoSchedulerDTO.class, id);
        }
        if (fifoSchedulerDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopFifoScheduler(fifoSchedulerDTO);
    }

    @Override
    public void prepare(Collection<HopFifoScheduler> modified, Collection<HopFifoScheduler> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopFifoScheduler hop : removed) {
                    FifoSchedulerClusterJ.FifoSchedulerDTO persistable = session.newInstance(FifoSchedulerClusterJ.FifoSchedulerDTO.class, hop.getId());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopFifoScheduler hop : modified) {
                    FifoSchedulerClusterJ.FifoSchedulerDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void createFifoScheduler(HopFifoScheduler fifoScheduler) throws StorageException {
        Session session = connector.obtainSession();
        createPersistable(fifoScheduler, session);
    }
    
    private HopFifoScheduler createHopFifoScheduler(FifoSchedulerDTO fifoSchedulerDTO) {
        return new HopFifoScheduler(
                fifoSchedulerDTO.getid(),
                fifoSchedulerDTO.getrmcontext(),
                fifoSchedulerDTO.getinitialized(),
                fifoSchedulerDTO.getuseportfornodename(),
                fifoSchedulerDTO.getqueuemetricsid(),
                fifoSchedulerDTO.getclusterresourceid(),
                fifoSchedulerDTO.getusedresourceid());
    }

    private FifoSchedulerClusterJ.FifoSchedulerDTO createPersistable(HopFifoScheduler fifoScheduler, Session session) {
        FifoSchedulerClusterJ.FifoSchedulerDTO fifoSchedulerDTO = session.newInstance(FifoSchedulerClusterJ.FifoSchedulerDTO.class);

        fifoSchedulerDTO.setid(fifoScheduler.getId());
        fifoSchedulerDTO.setrmcontex(fifoScheduler.getRmcontext());
        fifoSchedulerDTO.setinitialized(fifoScheduler.isInitialized());
        fifoSchedulerDTO.setuseportfornodename(fifoScheduler.isUseportfornodename());
        fifoSchedulerDTO.setqueuemetricsid(fifoScheduler.getQueuemetrics_id());
        fifoSchedulerDTO.setclusterresourceid(fifoScheduler.getClusterresource_id());
        fifoSchedulerDTO.setusedresourceid(fifoScheduler.getUsedresource_id());
        return fifoSchedulerDTO;
    }
    
}
