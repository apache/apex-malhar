Filter
=============

## Operator Objective
This operator receives an POJO ([Plain Old Java Object](https://en.wikipedia.org/wiki/Plain_Old_Java_Object)) as an incoming tuple
and based on the filter condition it emits filtered tuples on one output port and rest on another output port.

Filter operator supports quasi Java expressions to specify filter rule.

Filter operator does not hold any state and is **idempotent**, **fault-tolerant** and **statically/dynamically partitionable**.

## Operator Usecase
1. ***Customer data*** usually contains a field customer category/segment. One wants some analysis to be done for specific customer segment. One could use this filter operator to filter the records based on segment for some analysis for specific customer segment. 
2. ***Log data*** processing pipeline may want to filter logs from specific machine/router/switch.

## Operator Information
1. Operator location: ***[malhar-library](https://github.com/apache/apex-malhar/tree/master/library)***
2. Available since: ***3.5.0***
3. Operator state: ***Evolving***
3. Java Packages:
    * Operator: ***[com.datatorrent.lib.filter.FilterOperator](https://www.datatorrent.com/docs/apidocs/com/datatorrent/lib/filter/FilterOperator.html)***

## Properties, Attributes and Ports
### <a name="props"></a>Properties of FilterOperator
| **Property** | **Description** | **Type** | **Mandatory** | **Default Value** |
| -------- | ----------- | ---- | ------------------ | ------------- |
| *condition* | condition/expression with which Filtering is done. | String | Yes | N/A |
| *additionalExpressionFunctions* | List of import classes/method that should be made statically available to expression to use. | `List<String>`| No | Empty List |

### Platform Attributes that influences operator behavior
| **Attribute** | **Description** | **Type** | **Mandatory** |
| -------- | ----------- | ---- | ------------------ |
| *port.input.attr.TUPLE_CLASS* | TUPLE_CLASS attribute on input port indicates the class of POJO which incoming tuple | Class or FQCN| Yes |


### Ports
| **Port** | **Description** | **Type** | **Connection Required** |
| -------- | ----------- | ---- | ------------------ |
| *input* | Tuple which needs to be filtered are received on this port | Object (POJO) | Yes |
| *truePort* | Tuples which satisfies [condition](#props) are emitted on this port | Object (POJO) | No |
| *falsePort* | Tuples which does not satisfy [condition](#props) are emitted on this port | Object (POJO) | No |

## Limitations
Current `FilterOperator` has following limitation:

1. [APEXMALHAR-2175](https://issues.apache.org/jira/browse/APEXMALHAR-2175) : Filter condition is not able to correctly handle java reserved words.

## Example
Example for `FilterOperator` can be found at: [https://github.com/DataTorrent/examples/tree/master/tutorials/filter](https://github.com/DataTorrent/examples/tree/master/tutorials/filter)
