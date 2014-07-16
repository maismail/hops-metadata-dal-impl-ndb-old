/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.appmasterrpc.HopAppMasterRPC;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.yarn.dal.AppMasterRPCDataAccess;
import se.sics.hop.metadata.yarn.tabledef.appmasterrpc.AppMasterRPCTableDef;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class AppMasterRPCClusterJ implements AppMasterRPCTableDef, AppMasterRPCDataAccess<HopAppMasterRPC>{

    @PersistenceCapable(table = TABLE_NAME)
    public interface AppMasterRPCDTO {

        @PrimaryKey
        @Column(name = ID)
        int getid();
        void setid(int id);

        @Column(name = ENUM)
        String getenum();
        void setenum(String enumproto);

        @Column(name = PROTO)
        byte[] getproto();
        void setproto(byte[] proto);
    }
    private final ClusterjConnector connector = ClusterjConnector.getInstance();
    
    @Override
    public HopAppMasterRPC findById(int id) throws StorageException {
        Session session = connector.obtainSession();

        AppMasterRPCClusterJ.AppMasterRPCDTO appMasterRPCDTO = null;
        if (session != null) {
            appMasterRPCDTO = session.find(AppMasterRPCClusterJ.AppMasterRPCDTO.class, id);
        }
        if (appMasterRPCDTO == null) {
            throw new StorageException("HOP :: Error while retrieving row");
        }
        return createHopAppMasterRPC(appMasterRPCDTO);
    }

    @Override
    public void prepare(Collection<HopAppMasterRPC> modified, Collection<HopAppMasterRPC> removed) throws StorageException {
        Session session = connector.obtainSession();
        try {
            if (removed != null) {
                List<AppMasterRPCClusterJ.AppMasterRPCDTO> toRemove = new ArrayList<AppMasterRPCClusterJ.AppMasterRPCDTO>();
                for (HopAppMasterRPC hop : removed) {
                    toRemove.add(session.newInstance(AppMasterRPCClusterJ.AppMasterRPCDTO.class, hop.getId())); 
                }
                session.deletePersistentAll(toRemove);
            }
            if (modified != null) {
                List<AppMasterRPCClusterJ.AppMasterRPCDTO> toModify = new ArrayList<AppMasterRPCClusterJ.AppMasterRPCDTO>();
                for (HopAppMasterRPC hop : modified) {
                    toModify.add(createPersistable(hop, session)); 
                }
                session.savePersistentAll(toModify);
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppMasterRPC createHopAppMasterRPC(AppMasterRPCDTO appMasterRPCDTO) {
        return new HopAppMasterRPC(appMasterRPCDTO.getid(), appMasterRPCDTO.getenum(), appMasterRPCDTO.getproto());
    }
    
    private AppMasterRPCDTO createPersistable(HopAppMasterRPC hop, Session session) {
        AppMasterRPCClusterJ.AppMasterRPCDTO appMasterRPCDTO = session.newInstance(AppMasterRPCClusterJ.AppMasterRPCDTO.class);
        appMasterRPCDTO.setid(hop.getId());
        appMasterRPCDTO.setenum(hop.getEnumproto());
        appMasterRPCDTO.setproto(hop.getProto());
        
        return appMasterRPCDTO;
    }   
}
