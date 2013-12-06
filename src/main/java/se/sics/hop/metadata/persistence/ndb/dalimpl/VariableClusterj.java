package se.sics.hop.metadata.persistence.ndb.dalimpl;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import se.sics.hop.metadata.persistence.entity.hop.HopVariable;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.ndb.ClusterjConnector;
import se.sics.hop.metadata.persistence.tabledef.VariableTableDef;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class VariableClusterj implements VariableTableDef, VariableDataAccess<HopVariable, HopVariable.Finder> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface VariableDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = VARIABLE_VALUE)
    long getValue();

    void setValue(long value);
  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public HopVariable getVariable(HopVariable.Finder varType) throws StorageException {
    try {
      Session session = connector.obtainSession();
      VariableDTO var = session.find(VariableDTO.class, varType.getId());
      if (var == null) {
        throw new StorageException("There is no variable entry with id " + varType.getId());
      }
      return new HopVariable(varType, var.getValue());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<HopVariable> updatedVariables) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (HopVariable var : updatedVariables) {
        VariableDTO vd = session.newInstance(VariableDTO.class);
        vd.setValue(var.getValue());
        vd.setId(var.getType().getId());
        session.savePersistent(vd);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
}
