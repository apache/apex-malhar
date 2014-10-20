package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import com.datatorrent.lib.statistics.DimensionsComputation;

import java.util.Map;

/**
 * Performs dimensional computations given an event schema.
 * <p>
 * This class takes a schema description and uses that to generate aggregators internally during setup.  If schema does
 * not specify dimensions, then it generates aggregators for all combinations of keys.
 *
 * Schema can be specified as a JSON string with following keys.
 *
 *   fields: Map of all the field names and their types.  Supported types: java.lang.(Integer, Long, Float, Double, String)
 *   dimension: Array of dimensions with fields separated by colon, and time prefixed with time=.  Supported time units: MINUTES, HOURS, DAYS
 *   aggregates: Fields to aggregate for specified dimensions.  Aggregates types can include: sum, avg, min, max
 *   timestamp: Name of the timestamp field.  Data type should be Long with value in milliseconds since Jan 1, 1970 GMT.
 *
 * Example JSON schema for Ads demo:
 *
 *   {
 *     "fields": {"publisherId":"java.lang.Integer", "advertiserId":"java.lang.Integer", "adUnit":"java.lang.Integer", "clicks":"java.lang.Long", "price":"java.lang.Long", "cost":"java.lang.Double", "revenue":"java.lang.Double", "timestamp":"java.lang.Long"},
 *     "dimensions": ["time=MINUTES", "time=MINUTES:adUnit", "time=MINUTES:advertiserId", "time=MINUTES:publisherId", "time=MINUTES:advertiserId:adUnit", "time=MINUTES:publisherId:adUnit", "time=MINUTES:publisherId:advertiserId", "time=MINUTES:publisherId:advertiserId:adUnit"],
 *     "aggregates": { "clicks": "sum", "price": "sum", "cost": "sum", "revenue": "sum"},
 *     "timestamp": "timestamp"
 *   }

 *
 * @displayName Generic Dimension Computation
 * @category Math
 * @tags dimension, aggregation
 *
 */
public class GenericDimensionComputation extends DimensionsComputation<Object, GenericAggregate>
{
  private String eventSchemaJSON = EventSchema.DEFAULT_SCHEMA_SALES;
  private transient EventSchema eventSchema;

  // Initialize aggregators when this class is instantiated
  {
    initAggregators();
  }

  public String getEventSchemaJSON()
  {
    return eventSchemaJSON;
  }

  private void initAggregators(){
    DimensionsGenerator gen = new DimensionsGenerator(getEventSchema());
    Aggregator[] aggregators = gen.generateAggregators();
    setAggregators(aggregators);
  }

  public void setEventSchemaJSON(String eventSchemaJSON)
  {
    this.eventSchemaJSON = eventSchemaJSON;
    try {
      eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
    }
    initAggregators();
  }

  public EventSchema getEventSchema() {
    if (eventSchema == null ) {
      try {
        eventSchema = EventSchema.createFromJSON(eventSchemaJSON);
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse JSON input: " + eventSchemaJSON, e);
      }
    }
    return eventSchema;
  }


  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    initAggregators();
  }

  @Override
  public void processInputTuple(Object tuple)
  {
    GenericEvent ae = getEventSchema().convertMapToGenericEvent((Map<String, Object>) tuple);
    super.processInputTuple(ae);
  }
}