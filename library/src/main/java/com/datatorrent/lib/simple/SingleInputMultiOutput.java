package com.datatorrent.lib.simple;

import java.util.ArrayList;
import java.util.List;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.Name;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

/**
 * A base implementation of an operator that abstracts away the input and output ports.
 *
 * Subclasses should provide the implementation to process a tuple of type I and return a tuple of
 * type O such that the return value will emit to each output port in the array.
 *
 * <p>
 * <b>Input Port :</b><br>
 * <b> input :</b> default input port. <br>
 * <br>
 * <b>Output Port(s) :</b><br>
 * <b> output[] :</b> output port list that defaults to two ports. <br>
 * <br>
 * <b>Stateful : No</b>, all state is handled through the implementing class. <br>
 * <b>Partitions : Yes</b>, no dependency among input tuples. <br>
 * <br>
 * @displayName Single Input With Multiple Output
 * @category Simple Operators
 * @tags simple, single input, multiple output
 * @param <I> type being received from the input port
 * @param <O> type being sent from the output port
 * @since 3.3.0
 */
@Stateless
@Name("single-input-multi-output")
public abstract class SingleInputMultiOutput<I, O> extends BaseOperator
{
  protected static final int DEFAULT_NUM_OUTPUTS = 2;

  protected transient List<DefaultOutputPort<O>> outputs = null;

  protected final transient DefaultInputPort<I> input = new DefaultInputPort<I>() {
    @Override
    public void process(I inputTuple)
    {
      O result;
      if ((result = SingleInputMultiOutput.this.process(inputTuple)) != null) {
        for (int i = 0; i < outputs.size(); ++i) {
          outputs.get(i).emit(result);
        }
      }
    }
  };

  public SingleInputMultiOutput(int numOutputs) throws InstantiationException
  {
    if (numOutputs < 1 || numOutputs > 65535) {
      throw new InstantiationException("Cannot instantiate with " + numOutputs +
          "; must be 1 < numOutputs < 65535");
    } else {
      this.outputs = new ArrayList<DefaultOutputPort<O>>(numOutputs);

      for (int i = 0; i < numOutputs; ++i) {
        this.outputs.add(new DefaultOutputPort<O>());
      }
    }
  }

  public SingleInputMultiOutput() throws InstantiationException
  {
    this(DEFAULT_NUM_OUTPUTS);
  }

  public abstract O process(I inputTuple);
}
