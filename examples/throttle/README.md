Application shows you how to throttle input operators in the application when the downstream 
operators are slower.

It uses a combination of stats listener and operator request to achieve this. The throttler is 
a stats listener that is registered with the operators and when it notices that the window gap
between operators is widening and crosses a configured threshold, it sends a request to the
input operator to slow down and coversely when the gap falls below the threshold it requests
the input operator to go back to its normal speed.
