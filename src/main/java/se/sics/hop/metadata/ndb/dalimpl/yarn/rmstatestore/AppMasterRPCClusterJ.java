/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.zip.DataFormatException;

import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.appmasterrpc.HopAppMasterRPC;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsQuery;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryBuilder;
import se.sics.hop.metadata.ndb.wrapper.HopsQueryDomainType;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.rmstatestore.AppMasterRPCDataAccess;
import se.sics.hop.metadata.yarn.tabledef.appmasterrpc.AppMasterRPCTableDef;
import se.sics.hop.util.CompressionUtils;

/**
 *
 * @author Nikos Stanogias <niksta@sics.se>
 */
public class AppMasterRPCClusterJ implements AppMasterRPCTableDef,
        AppMasterRPCDataAccess<HopAppMasterRPC> {

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

    @Column(name = USERID)
    String getuserId();

    void setuserId(String userid);

  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopAppMasterRPC findById(int id) throws StorageException {
    HopsSession session = connector.obtainSession();

    AppMasterRPCClusterJ.AppMasterRPCDTO appMasterRPCDTO = null;
    if (session != null) {
      appMasterRPCDTO = session.find(AppMasterRPCClusterJ.AppMasterRPCDTO.class,
              id);
    }
    if (appMasterRPCDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }
    return createHopAppMasterRPC(appMasterRPCDTO);
  }

  @Override
  public void prepare(Collection<HopAppMasterRPC> modified,
          Collection<HopAppMasterRPC> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        List<AppMasterRPCClusterJ.AppMasterRPCDTO> toRemove
                = new ArrayList<AppMasterRPCClusterJ.AppMasterRPCDTO>();
        for (HopAppMasterRPC hop : removed) {
          toRemove.add(session.newInstance(
                  AppMasterRPCClusterJ.AppMasterRPCDTO.class, hop.getId()));
        }
        session.deletePersistentAll(toRemove);
      }
      if (modified != null) {
        List<AppMasterRPCClusterJ.AppMasterRPCDTO> toModify
                = new ArrayList<AppMasterRPCClusterJ.AppMasterRPCDTO>();
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
      HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<AppMasterRPCClusterJ.AppMasterRPCDTO> dobj = qb.
              createQueryDefinition(AppMasterRPCClusterJ.AppMasterRPCDTO.class);
            //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
      //dobj.where(pred1);
      HopsQuery<AppMasterRPCClusterJ.AppMasterRPCDTO> query = session.createQuery(
              dobj);
      //query.setParameter("applicationid", applicationid);
      List<AppMasterRPCClusterJ.AppMasterRPCDTO> results = query.getResultList();
//            if (results != null && !results.isEmpty()) {
      return createHopAppMasterRPCList(results);
//            } else {

//            }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private HopAppMasterRPC createHopAppMasterRPC(AppMasterRPCDTO appMasterRPCDTO)
      throws StorageException {
    try {
      return new HopAppMasterRPC(
          appMasterRPCDTO.getid(),
          HopAppMasterRPC.Type.valueOf(appMasterRPCDTO.gettype()),
          CompressionUtils.decompress(appMasterRPCDTO.getrpc()),
          appMasterRPCDTO.getuserId());
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private List<HopAppMasterRPC> createHopAppMasterRPCList(
          List<AppMasterRPCClusterJ.AppMasterRPCDTO> list)
      throws StorageException {
    List<HopAppMasterRPC> hopList = new ArrayList<HopAppMasterRPC>();
    for (AppMasterRPCClusterJ.AppMasterRPCDTO dto : list) {
      hopList.add(createHopAppMasterRPC(dto));
    }
    return hopList;

  }

  private AppMasterRPCDTO createPersistable(HopAppMasterRPC hop, HopsSession session)
      throws StorageException {
    AppMasterRPCClusterJ.AppMasterRPCDTO appMasterRPCDTO = session.newInstance(
            AppMasterRPCClusterJ.AppMasterRPCDTO.class);
    appMasterRPCDTO.setid(hop.getId());
    appMasterRPCDTO.settype(hop.getType().name());
    try {
      appMasterRPCDTO.setrpc(CompressionUtils.compress(hop.getRpc()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    appMasterRPCDTO.setuserId(hop.getUserId());

    return appMasterRPCDTO;
  }
}
