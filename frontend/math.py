import numpy as np

def rolling_weighted_triangle_conv(x, w, window_size):
  """Smooth with triangle window, also using per-element weights."""

  x = np.asarray(x)
  w = np.asarray(w)

  # Simplify slicing
  wing = window_size // 2

  # Pad both arrays with mirror-image values at edges
  xp = np.concatenate((x[wing-1::-1], x, x[:-wing-1:-1]))
  wp = np.concatenate((w[wing-1::-1], w, w[:-wing-1:-1]))

  # Generate a (triangular) window of weights to slide
  incr = 1. / (wing + 1)
  ramp = np.arange(incr, 1, incr)
  triangle = np.r_[ramp, 1.0, ramp[::-1]]

  D = np.convolve(wp*xp, triangle)[window_size-1:-window_size+1]
  N = np.convolve(wp, triangle)[window_size-1:-window_size+1]    
  return D/N
