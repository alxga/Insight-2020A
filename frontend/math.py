import numpy as np

__author__ = "Alex Ganin"


def rolling_weighted_triangle_conv(values, weights, window_size):
  """Smoothes the data with a window and given the weights

  Original algorithm: stackoverflow.com/a/46232913/1257075

    Returns:
      Smoothed values

    Args:
      values: data to smoothen
      weights: weights for each data point
      window_size: window size for the smoothing algorithm
  """

  y = np.asarray(values)
  w = np.asarray(weights)

  # Simplify slicing
  wing = window_size // 2

  # Pad both arrays with mirror-image values at edges
  yp = np.concatenate((y[wing-1::-1], y, y[:-wing-1:-1]))
  wp = np.concatenate((w[wing-1::-1], w, w[:-wing-1:-1]))

  # Generate a (triangular) window of weights to slide
  incr = 1. / (wing + 1)
  ramp = np.arange(incr, 1, incr)
  triangle = np.r_[ramp, 1.0, ramp[::-1]]

  D = np.convolve(wp * yp, triangle)[window_size-1:-window_size+1]
  N = np.convolve(wp, triangle)[window_size-1:-window_size+1]
  return D / N
