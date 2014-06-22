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
import se.sics.hop.metadata.hdfs.entity.yarn.HopResponseMap;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.ResponseMapDataAccess;
import se.sics.hop.metadata.yarn.tabledef.ResponseMapTableDef;

/**
 *
 * @author nickstanogias
 */
public class ResponseMapClusterJ implements ResponseMapTableDef, ResponseMapDataAccess<HopResponseMap>{


    @PersistenceCapable(table = TABLE_NAME)
    public interface ResponseMapDTO {

        @PrimaryKey
        @Column(name = APPMASTERSERVICE_ID)
        int getappmasterserviceid();
        void setappmasterserviceid(int appmasterserviceid);

        @Column(name = APPATTEMPTID_ID)
        int getappattemptid();
        void setappattemptid(int appattemptid);
        
        @Column(name = ALLOCATERESPONSE_ID)
        int getallocationresponseid();
        void setallocationresponseid(int allocationresponseid);
        
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopResponseMap findById(int id) throws StorageException {
         Session session = connector.obtainSession();

        ResponseMapClusterJ.ResponseMapDTO responseMapDTO = null;
        if (session != null) {
            responseMapDTO = session.find(ResponseMapClusterJ.ResponseMapDTO.class, id);
        }
        if (responseMapDTO == null) {
                throw new StorageException("HOP :: Error while retrieving row");
        }

        return createHopResponseMap(responseMapDTO);
    }

    @Override
    public void prepare(Collection<HopResponseMap> modified, Collection<HopResponseMap> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                for (HopResponseMap hop : removed) {
                    ResponseMapClusterJ.ResponseMapDTO persistable = session.newInstance(ResponseMapClusterJ.ResponseMapDTO.class, hop.getAppmasterservive_id());
                    session.deletePersistent(persistable);
                }
            }
            if (modified != null) {
                for (HopResponseMap hop : modified) {
                    ResponseMapClusterJ.ResponseMapDTO persistable = createPersistable(hop, session);
                    session.savePersistent(persistable);
                }
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopResponseMap createHopResponseMap(ResponseMapDTO responseMapDTO) {
        return new HopResponseMap(responseMapDTO.getappmasterserviceid(),
                                    responseMapDTO.getappattemptid(),
                                    responseMapDTO.getallocationresponseid());
    }

    private ResponseMapDTO createPersistable(HopResponseMap hop, Session session) {
        ResponseMapClusterJ.ResponseMapDTO responseMapDTO = session.newInstance(ResponseMapClusterJ.ResponseMapDTO.class);
        
        responseMapDTO.setallocationresponseid(hop.getAllocateresponse_id());
        responseMapDTO.setappattemptid(hop.getAppattemptid_id());
        responseMapDTO.setappmasterserviceid(hop.getAppmasterservive_id());
        
        return responseMapDTO;
        
    }
}
