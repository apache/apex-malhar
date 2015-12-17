package com.datatorrent.lib.complex;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Name;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * A base implementation of an operator that abstracts away the input and output ports.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a tuple of
 * type O such that for each input port the tuple is directly mapped to its corresponding output
 * port based on their equivalent indices. Input ports and output ports are always guaranteed to be
 * balanced (equal).
 *
 * <p>
 * <b>Input Port(s) :</b><br>
 * <b> input[] :</b> input port list with a default of two ports. <br>
 * <br>
 * <b>Output Port(s) :</b><br>
 * <b> output[] :</b> output port list with a default of two ports. <br>
 * <br>
 * <b>Stateful : No</b>, all state is handled through the implementing class. <br>
 * <b>Partitions : Yes</b>, no dependency among input tuples. <br>
 * <br>
 * @displayName Direct Multiple Input & Output
 * @category Complex Operators
 * @tags complex, direct, multiple input, multiple output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("direct-multi-input-output")
public abstract class DirectMultiInputOutput<I, O> extends BaseOperator {
  protected static final int DEFAULT_NUM_INPUTS_OUTPUTS = 2;

  protected transient List<DefaultInputPort<I>> inputs = null;
  protected transient List<DefaultOutputPort<O>> outputs = null;

  public DirectMultiInputOutput() throws InstantiationException {
    this(DEFAULT_NUM_INPUTS_OUTPUTS);
  }

  public DirectMultiInputOutput(int numInputsOutputs) throws InstantiationException {
    if (numInputsOutputs < 1 || numInputsOutputs > 65535) {
      throw new InstantiationException("Cannot instantiate with "+numInputsOutputs+
          "; must be 1 < numInputsOutputs < 65535");
    } else {
      inputs = new ArrayList<DefaultInputPort<I>>(numInputsOutputs);
      outputs = new ArrayList<DefaultOutputPort<O>>(numInputsOutputs);

      for (int i = 0; i < numInputsOutputs; ++i) {
        final DefaultOutputPort<O> output = new DefaultOutputPort<O>();
        outputs.add(output);

        inputs.add(new DefaultInputPort<I>() {
          @Override
          public void process(I inputTuple) {
            O result;
            if ((result = DirectMultiInputOutput.this.process(inputTuple)) != null) {
              output.emit(result);
            }
          }
        });
      }
    }
  }

  public abstract O process(I inputTuple);
}
