package com.datatorrent.lib.simple;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Name;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * A base implementation of an operator that abstracts away the input ports and output port.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a tuple of
 * type O such that each value from the input ports maps to the single output port.
 *
 * <p>
 * <b>Input Port(s) :</b><br>
 * <b> input[] :</b> input port list that defaults to two ports. <br>
 * <br>
 * <b>Output Port :</b><br>
 * <b> output :</b> default output port. <br>
 * <br>
 * <b>Stateful : No</b>, all state is handled through the implementing class. <br>
 * <b>Partitions : Yes</b>, no dependency among input tuples. <br>
 * <br>
 * @displayName Multiple Input With Single Output
 * @category Simple Operators
 * @tags simple, multiple input, single output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("multi-input-single-output")
public abstract class MultiInputSingleOutput<I, O> extends BaseOperator {
  protected static final int DEFAULT_NUM_INPUTS = 2;

  protected transient final DefaultOutputPort<O> output = new DefaultOutputPort<O>();

  protected transient List<DefaultInputPort<I>> inputs = null;

  public MultiInputSingleOutput(int numInputs) throws InstantiationException {
    if (numInputs < 1 || numInputs > 65535) {
      throw new InstantiationException("Cannot instantiate with "+numInputs+
          "; must be 1 < numInputs < 65535");
    } else {
      this.inputs = new ArrayList<DefaultInputPort<I>>(numInputs);

      for (int i = 0; i < numInputs; ++i) {
        inputs.add(new DefaultInputPort<I>() {
          @Override
          public void process(I i) {
            O result;
            if ((result = MultiInputSingleOutput.this.process(i)) != null) {
              output.emit(result);
            }
          }
        });
      }
    }
  }

  public MultiInputSingleOutput() throws InstantiationException {
    this(DEFAULT_NUM_INPUTS);
  }

  public abstract O process(I inputTuple);
}
