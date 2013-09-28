/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.machinedata;

import com.datatorrent.lib.util.TimeBucketKey;

import java.util.Calendar;
import java.util.List;

/**
 * <p>MachineKey class.</p>
 *
 * @since 0.3.5
 */
public class MachineKey extends TimeBucketKey {

    private Integer customer;
    private Integer product;
    private Integer os;
    private Integer software1;
    private Integer software2;
    private Integer software3;

    public MachineKey() {
    }

    public MachineKey(Calendar time, int timeSpec) {
        super(time, timeSpec);
    }

    public MachineKey(Calendar time, Integer timeSpec,  Integer customer, Integer product, Integer os, Integer software1, Integer software2, Integer software3) {
        super(time, timeSpec);
        this.customer = customer;
        this.product = product;
        this.os = os;
        this.software1 = software1;
        this.software2 = software2;
        this.software3 = software3;
    }

    public Integer getCustomer() {
        return customer;
    }

    public void setCustomer(Integer customer) {
        this.customer = customer;
    }

    public Integer getProduct() {
        return product;
    }

    public void setProduct(Integer product) {
        this.product = product;
    }

    public Integer getOs() {
        return os;
    }

    public void setOs(Integer os) {
        this.os = os;
    }

    public Integer getSoftware1() {
        return software1;
    }

    public void setSoftware1(Integer software1) {
        this.software1 = software1;
    }

    public Integer getSoftware2() {
        return software2;
    }

    public void setSoftware2(Integer software2) {
        this.software2 = software2;
    }

    public Integer getSoftware3() {
        return software3;
    }

    public void setSoftware3(Integer software3) {
        this.software3 = software3;
    }

    @Override
    public int hashCode() {
        int key = 0;
        if (customer != null) {
            key |= (1 << 23);
            key |= (customer << 16);
        }
        if (product != null) {
            key |= (1 << 22);
            key |= (product << 15);
        }
        if (os != null) {
            key |= (1 << 21);
            key |= (os << 14);
        }
        if (software1 != null) {
            key |= (1 << 20);
            key |= (software1 << 13);
        }
        if (software2 != null) {
            key |= (1 << 19);
            key |= (software2 << 13);
        }
        if (software3 != null) {
            key |= (1 << 18);
            key |= (software3 << 13);
        }
        return super.hashCode() ^ key;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MachineKey)) {
            return false;
        }
        MachineKey mkey = (MachineKey) obj;
        return super.equals(obj)
                && checkIntEqual(this.customer, mkey.customer)
                && checkIntEqual(this.product, mkey.product)
                && checkIntEqual(this.os, mkey.os)
                && checkIntEqual(this.software1, mkey.software1)
                && checkIntEqual(this.software2, mkey.software2)
                && checkIntEqual(this.software3, mkey.software3);
    }

    private boolean checkIntEqual(Integer a, Integer b) {
      if ((a == null) && (b == null)) return true;
      if ((a != null) && a.equals(b)) return true;
      return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        if (customer != null) sb.append("|0:").append(customer);
        if (product != null) sb.append("|1:").append(product);
        if (os != null) sb.append("|2:").append(os);
        if (software1 != null) sb.append("|3:").append(software1);
        if (software2 != null) sb.append("|4:").append(software2);
        if (software3 != null) sb.append("|5:").append(software3);
        return sb.toString();
    }

}
