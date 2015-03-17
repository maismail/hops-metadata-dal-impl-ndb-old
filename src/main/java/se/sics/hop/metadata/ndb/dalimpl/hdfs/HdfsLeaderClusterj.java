/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.hop.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.security.InvalidParameterException;
import se.sics.hop.metadata.hdfs.entity.hop.election.LeDescriptor;
import se.sics.hop.metadata.hdfs.entity.hop.election.LeDescriptor.HdfsLeDescriptor;

import se.sics.hop.metadata.hdfs.tabledef.HdfsLeaderTableDef;

public class HdfsLeaderClusterj extends LeDescriptorClusterj implements HdfsLeaderTableDef {

    @PersistenceCapable(table = TABLE_NAME)
    public interface HdfsLeaderDTO extends LeaderDTO{

        @PrimaryKey
        @Column(name = ID)
        @Override
        long getId();

        @Override
        void setId(long id);

        @PrimaryKey
        @Column(name = PARTITION_VAL)
        @Override
        int getPartitionVal();

        @Override
        void setPartitionVal(int partitionVal);

        @Column(name = COUNTER)
        @Override
        long getCounter();

        @Override
        void setCounter(long counter);

        @Column(name = HOSTNAME)
        @Override
        String getHostname();

        @Override
        void setHostname(String hostname);

        @Column(name = HTTP_ADDRESS)
        @Override
        String getHttpAddress();

        @Override
        void setHttpAddress(String httpAddress);  
    }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new HdfsLeDescriptor(lTable.getId(),
            lTable.getCounter(),
            lTable.getHostname(),
            lTable.getHttpAddress());
  }
 
    public HdfsLeaderClusterj(){
        super(HdfsLeaderDTO.class);
    }
}
