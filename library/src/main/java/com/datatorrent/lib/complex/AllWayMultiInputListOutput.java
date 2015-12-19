package com.datatorrent.lib.complex;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Name;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * A base implementation of an operator that abstracts away the input and output port.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a list of
 * tuples of type O such that each element in the list will be output to its corresponding output
 * port based on their equal indices (i.e. output[0] receives tuple[0], output[1] receives tuple[1],
 * etc.). This operator is equivalent to {@link com.datatorrent.lib.complex.SingleInputListOutput
 * SingleInputOutputList} except allowing multiple input ports.
 *
 * In the event that the tuple list is smaller than the number of output ports the values are
 * deterministically emitted out to each port until the list is exhausted starting with the first
 * element. In the case where the tuple list is larger than the number of output ports only the
 * first tuples that correctly match their indices with the output ports are emitted and all else
 * are wasted.
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
 * @displayName All Way Multiple Input With List Output
 * @category Complex Operators
 * @tags complex, multiple input, list output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("all-way-multiple-input-list-output")
public abstract class AllWayMultiInputListOutput<I, O> extends BaseOperator
{
  protected static final int DEFAULT_NUM_INPUTS = 2;
  protected static final int DEFAULT_NUM_OUTPUTS = 2;

  protected transient List<DefaultInputPort<I>> inputs = null;
  protected transient List<DefaultOutputPort<O>> outputs = null;

  public AllWayMultiInputListOutput() throws InstantiationException
  {
    this(DEFAULT_NUM_INPUTS, DEFAULT_NUM_OUTPUTS);
  }

  public AllWayMultiInputListOutput(int numInputs, int numOutputs) throws InstantiationException
  {
    if (numInputs < 1 || numInputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numInputs +
          "; must be 1 < numInputs < 65535");
    } else if (numOutputs < 1 || numOutputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numOutputs +
          "; must be 1 < numOutputs < 65535");
    } else {
      inputs = new ArrayList<DefaultInputPort<I>>(numInputs);
      outputs = new ArrayList<DefaultOutputPort<O>>(numOutputs);

      for (int i = 0; i < numOutputs; ++i) {
        outputs.add(new DefaultOutputPort<O>());
      }

      for (int i = 0; i < numInputs; ++i) {
        inputs.add(new DefaultInputPort<I>() {
          @Override
          public void process(I inputTuple)
          {
            List<O> results = AllWayMultiInputListOutput.this.process(inputTuple);

            for (int i = 0; i < outputs.size(); ++i) {
              O result;
              if ((result = results.get(i)) != null) {
                outputs.get(i).emit(result);
              }
            }
          }
        });
      }
    }
  }

  public abstract List<O> process(I inputTuple);
}
