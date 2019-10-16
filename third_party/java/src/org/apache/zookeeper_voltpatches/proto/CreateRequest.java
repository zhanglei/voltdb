// File generated by hadoop record compiler. Do not edit.
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zookeeper_voltpatches.proto;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jute_voltpatches.*;
import org.apache.zookeeper_voltpatches.data.ACL;

public class CreateRequest implements Record, Comparable<CreateRequest> {
    private String path;
    private byte[] data;
    private List<ACL> acl;
    private int flags;
    public CreateRequest() {
    }
    public String getPath() {
        return path;
    }
    public void setPath(String m_) {
        path=m_;
    }
    public byte[] getData() {
        return data;
    }
    public void setData(byte[] m_) {
        data=m_;
    }
    public List<ACL> getAcl() {
        return acl;
    }
    public void setAcl(List<ACL> m_) {
        acl=m_;
    }
    public int getFlags() {
        return flags;
    }
    public void setFlags(int m_) {
        flags=m_;
    }
    public void serialize(OutputArchive a_, String tag) throws IOException {
        a_.startRecord(this,tag);
        a_.writeString(path,"path");
        a_.writeBuffer(data,"data");
        {
            a_.startVector(acl,"acl");
            if (acl!= null) {
                for (ACL e1 : acl) {
                    a_.writeRecord(e1, "e1");
                }
            }
            a_.endVector(acl,"acl");
        }
        a_.writeInt(flags,"flags");
        a_.endRecord(this,tag);
    }
    public void deserialize(InputArchive a_, String tag) throws java.io.IOException {
        a_.startRecord(tag);
        path=a_.readString("path");
        data=a_.readBuffer("data");
        {
            Index vidx1 = a_.startVector("acl");
            if (vidx1 != null) {
                acl = new ArrayList<>();
                for (; !vidx1.done(); vidx1.incr()) {
                    ACL e1 = new ACL();
                    a_.readRecord(e1,"e1");
                    acl.add(e1);
                }
            }
            a_.endVector("acl");
        }
        flags=a_.readInt("flags");
        a_.endRecord(tag);
    }

    @Override
    public String toString() {
        try {
            ByteArrayOutputStream s = new ByteArrayOutputStream();
            CsvOutputArchive a_ = new CsvOutputArchive(s);
            a_.startRecord(this,"");
            a_.writeString(path,"path");
            a_.writeBuffer(data,"data");
            {
                a_.startVector(acl,"acl");
                if (acl!= null) {
                    for (ACL e1 : acl) {
                        a_.writeRecord(e1, "e1");
                    }
                }
                a_.endVector(acl,"acl");
            }
            a_.writeInt(flags,"flags");
            a_.endRecord(this,"");
            return new String(s.toByteArray(), StandardCharsets.UTF_8);
        } catch (Throwable ex) {
            ex.printStackTrace();
            return "ERROR";
        }
    }

    public void write(DataOutput out) throws IOException {
        serialize(new BinaryOutputArchive(out), "");
    }

    public void readFields(DataInput in) throws IOException {
        deserialize(new BinaryInputArchive(in), "");
    }
    @Override
    public int compareTo(CreateRequest ignored) {
        throw new UnsupportedOperationException("comparing CreateRequest is unimplemented");
    }

    @Override
    public boolean equals(Object peer_) {
        if (! (peer_ instanceof CreateRequest)) {
            return false;
        } else if (peer_ == this) {
            return true;
        }
        final CreateRequest peer = (CreateRequest) peer_;
        return path.equals(peer.path) &&
                Utils.bufEquals(data, peer.data) &&
                acl.equals(peer.acl) &&
                flags == peer.flags;
    }

    @Override
    public int hashCode() {
        int result = 17;
        int ret = path.hashCode();
        result = 37*result + ret;
        ret = Arrays.toString(data).hashCode();
        result = 37*result + ret;
        ret = acl.hashCode();
        result = 37*result + ret;
        ret = flags;
        return  37*result + ret;
    }

    public static String signature() {
        return "LCreateRequest(sB[LACL(iLId(ss))]i)";
    }
}
