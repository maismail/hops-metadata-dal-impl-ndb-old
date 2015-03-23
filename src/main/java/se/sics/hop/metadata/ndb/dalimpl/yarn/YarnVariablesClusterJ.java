package se.sics.hop.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.yarn.HopsYarnVariables;
import se.sics.hop.metadata.ndb.ClusterjConnector;
import se.sics.hop.metadata.ndb.wrapper.HopsSession;
import se.sics.hop.metadata.yarn.dal.YarnVariablesDataAccess;
import se.sics.hop.metadata.yarn.tabledef.YarnVariablesTableDef;

/**
 * Table with one row that is used to obtain unique ids for tables. This
 * solution can be dropped once ClusterJ implements auto-increment.
 */
public class YarnVariablesClusterJ implements YarnVariablesTableDef,
        YarnVariablesDataAccess<HopsYarnVariables> {

  private static final Log LOG = LogFactory.getLog(YarnVariablesClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnVariablesDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();

    void setid(int id);

    @Column(name = VALUE)
    int getvalue();

    void setvalue(int value);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopsYarnVariables findById(int id) throws StorageException {
    LOG.debug("HOP :: ClusterJ YarnVariables.findById - START:" + id);
    HopsSession session = connector.obtainSession();
    YarnVariablesDTO yarnDTO;
    if (session != null) {
      if (id == Integer.MIN_VALUE) {
        id = idVal;
      }
      yarnDTO = session.find(YarnVariablesDTO.class, id);
      LOG.debug("HOP :: ClusterJ YarnVariables.findById - FINISH:" + id);
      if (yarnDTO != null) {
        return new HopsYarnVariables(yarnDTO.getid(), yarnDTO.getvalue());
      }
    }
    return null;
  }

  @Override
  public void add(HopsYarnVariables toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  private YarnVariablesDTO createPersistable(HopsYarnVariables yarnVariables,
          HopsSession session) throws StorageException {
    YarnVariablesDTO yarnDTO = session.newInstance(YarnVariablesDTO.class);
    yarnDTO.setid(yarnVariables.getId());
    yarnDTO.setvalue(yarnVariables.getValue());
    return yarnDTO;
  }
}
