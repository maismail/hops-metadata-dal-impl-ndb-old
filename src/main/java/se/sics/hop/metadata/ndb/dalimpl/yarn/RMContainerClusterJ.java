package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopRMContainer;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.RMContainerDataAccess;
import se.sics.hop.metadata.yarn.tabledef.RMContainerTableDef;

/**
 *
 * @author Theofilos Kakantousis <tkak@sics.se>
 */
public class RMContainerClusterJ implements RMContainerTableDef, RMContainerDataAccess<HopRMContainer> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface ApplicationIdDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();

        void setid(int id);

        @Column(name = CONTAINERID_ID)
        int getcontaineridid();

        void setcontaineridid(int containeridid);

        @Column(name = APPLICATIONATTEMPTID_ID)
        int getappattemptidid();

        void setappattemptidid(int appattemptidid);

        @Column(name = NODEID_ID)
        int getnodeidid();

        void setnodeidid(int nodeidid);

        @Column(name = CONTAINER_ID)
        int getcontainerid();

        void setcontainerid(int containerid);

        @Column(name = RESERVED_RESOURCE_ID)
        int getreservedresourceid();

        void setreservedresourceid(int reservedresourceid);

        @Column(name = RESERVED_PRIORITY_ID)
        int getreservedpriorityid();

        void setreservedpriorityid(int reservedpriorityid);

        @Column(name = STATE)
        String getstate();

        void setstate(String state);

        @Column(name = NEWLY_ALLOCATED)
        int getnewlyallocated();

        void setnewlyallocated(int newlyallocated);
    }
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public HopRMContainer findById(int id) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void prepare(Collection<HopRMContainer> modified, Collection<HopRMContainer> removed) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void createRMContainer(HopRMContainer rmcontainer) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
