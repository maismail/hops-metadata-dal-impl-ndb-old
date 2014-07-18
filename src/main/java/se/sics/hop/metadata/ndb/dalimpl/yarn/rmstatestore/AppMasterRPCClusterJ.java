/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.Query;
import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import com.mysql.clusterj.query.QueryBuilder;
import com.mysql.clusterj.query.QueryDomainType;
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

        @Column(name = TYPE)
        String gettype();
        void settype(String type);

        @Column(name = RPC)
        byte[] getrpc();
        void setrpc(byte[] rpc);
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
    
    @Override
    public List<HopAppMasterRPC> getAll() throws StorageException {
        try {
            Session session = connector.obtainSession();
            QueryBuilder qb = session.getQueryBuilder();
            QueryDomainType<AppMasterRPCClusterJ.AppMasterRPCDTO> dobj = qb.createQueryDefinition(AppMasterRPCClusterJ.AppMasterRPCDTO.class);
            //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
            //dobj.where(pred1);
            Query<AppMasterRPCClusterJ.AppMasterRPCDTO> query = session.createQuery(dobj);
            //query.setParameter("applicationid", applicationid);
            List<AppMasterRPCClusterJ.AppMasterRPCDTO> results = query.getResultList();
            if (results != null && !results.isEmpty()) {
                return createHopAppMasterRPCList(results);
            } else {
                throw new StorageException("HOP :: Error retrieving AppMasterRPCs");
            }
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    
    private HopAppMasterRPC createHopAppMasterRPC(AppMasterRPCDTO appMasterRPCDTO) {
        return new HopAppMasterRPC(appMasterRPCDTO.getid(), appMasterRPCDTO.gettype(), appMasterRPCDTO.getrpc());
    }
    
    private List<HopAppMasterRPC> createHopAppMasterRPCList(List<AppMasterRPCClusterJ.AppMasterRPCDTO> list) {
        List<HopAppMasterRPC> hopList = new ArrayList<HopAppMasterRPC>();
        for (AppMasterRPCClusterJ.AppMasterRPCDTO dto : list) {
            hopList.add(createHopAppMasterRPC(dto));
        }
        return hopList;

    }
    
    private AppMasterRPCDTO createPersistable(HopAppMasterRPC hop, Session session) {
        AppMasterRPCClusterJ.AppMasterRPCDTO appMasterRPCDTO = session.newInstance(AppMasterRPCClusterJ.AppMasterRPCDTO.class);
        appMasterRPCDTO.setid(hop.getId());
        appMasterRPCDTO.settype(hop.getType());
        appMasterRPCDTO.setrpc(hop.getRpc());
        
        return appMasterRPCDTO;
    }   
}
