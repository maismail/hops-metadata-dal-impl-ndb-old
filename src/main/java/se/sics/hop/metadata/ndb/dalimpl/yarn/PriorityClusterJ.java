package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopPriority;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.PriorityDataAccess;
import se.sics.hop.metadata.yarn.tabledef.PriorityTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class PriorityClusterJ implements PriorityTableDef, PriorityDataAccess<HopPriority> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface FifoSchedulerNodesDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = PRIORITY)
        int getpriorityid();

        void setpriorityid(int priorityid);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopPriority findById(int id) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Collection<HopPriority> modified, Collection<HopPriority> removed) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void createPriority(HopPriority priority) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
