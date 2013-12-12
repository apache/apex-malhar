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
package com.datatorrent.contrib.netflow;

/**
 * This defines the field definition for the Netflow V9
 *
 * @since 0.9.2
 */
public class FieldDefinition_V9 {
	public static int InBYTES_32 = 1;

	public static int InPKTS_32 = 2;

	public static int FLOWS = 3;

	public static int PROT = 4;

	public static int SRC_TOS = 5;

	public static int TCP_FLAGS = 6;

	public static int L4_SRC_PORT = 7;

	public static int IPV4_SRC_ADDR = 8;

	public static int SRC_MASK = 9;

	public static int INPUT_SNMP = 10;

	public static int L4_DST_PORT = 11;

	public static int IPV4_DST_ADDR = 12;

	public static int DST_MASK = 13;

	public static int OUTPUT_SNMP = 14;

	public static int IPV4_NEXT_HOP = 15;

	public static int SRC_AS = 16;

	public static int DST_AS = 17;

	public static int BGP_NEXT_HOP = 18;

	public static int IPM_DPKTS = 19;

	public static int IPM_DOCTETS = 20;

	public static int LAST_SWITCHED = 21;

	public static int FIRST_SWITCHED = 22;

	public static int BYTES_64 = 25;

	public static int PKTS_64 = 24;

	public static int MAC_ADDR = 25;

	public static int VLAN_ID = 26;

	public static int IPV6_SRC_ADDR = 27;

	public static int IPV6_DST_ADDR = 28;

	public static int IPV6_SRC_MASK = 29;

	public static int IPV6_DST_MASK = 30;

	public static int FLOW_LABEL = 31;

	public static int ICMP_TYPE = 32;

	public static int MUL_IGMP_TYPE = 33;

	public static int SAMPLING_INTERVAL = 34;

	public static int SAMPLING_ALGORITHM = 35;

	public static int FLOW_ACTIVE_TIMEOUT = 36;

	public static int FLOW_INACTIVE_TIMEOUT = 37;

	public static int ENGINE_TYPE = 38;

	public static int ENGINE_ID = 39;

	public static int TOTAL_BYTES_EXPORTED = 40;

	public static int TOTAL_EXPORT_PKTS_SENT = 41;

	public static int TOTAL_FLOWS_EXPORTED = 42;

	public static int FLOW_SAMPLER_ID = 48;

	public static int FLOW_SAMPLER_MODE = 49;

	public static int FLOW_SAMPLER_RANDOM_INTERVAL = 50;

	public static int IP_PROTOCOL_VERSION = 60;

	public static int DIRECTION = 61;

	public static int IPV6_NEXT_HOP = 62;

	public static int BGP_IPV6_NEXT_HOP = 63;

	public static int IPV6_OPTION_HEADERS = 64;

	public static int MPLS_LABEL_1 = 70;

	public static int MPLS_LABEL_2 = 71;

	public static int MPLS_LABEL_3 = 72;

	public static int MPLS_LABEL_4 = 73;

	public static int MPLS_LABEL_5 = 74;

	public static int MPLS_LABEL_6 = 75;

	public static int MPLS_LABEL_7 = 76;

	public static int MPLS_LABEL_8 = 77;

	public static int MPLS_LABEL_9 = 78;

	public static int MPLS_LABEL_10 = 79;

	public static int IN_DST_MAC = 80;

	public static int OUT_SRC_MAC = 81;

	public static int IF_NAME = 82;

	public static int IF_DESC = 83;

	public static int SAMPLER_NAME = 84;

	public static int IN_PERMANENT_BYTES = 85;

	public static int IN_PERMANENT_PKTS = 86;
}
