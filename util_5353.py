import os
import traceback

def die(prob, msg, params):
  print('[Problem ' + prob + ']: ' + msg % params)
  # Uncomment the following to enable stack traces:
  #print('')
  #print('Stack Trace:')
  #traceback.print_stack()
  exit(1)

def assert_not_none(v, prob):
  if v is None:
    die(prob, 'Value should not be None')

def assert_int(v, prob):
  if type(v) != int:
    die(prob, 'Value should be an integer, found %s', type(v))

def assert_int_eq(gold_int, guess_int, prob):
  if gold_int != guess_int:
    die(prob, 'Value should be %d, found %d', (gold_int, guess_int))

def assert_int_range(int_range, v, prob):
  assert_int(v, prob)
  if v < int_range[0] or v > int_range[1]:
    die(prob, 'Value should be in range [%d,%d], found %d',
        (int_range[0], int_range[1], v))

def assert_float(v, prob):
  if type(v) != float:
    die(prob, 'Value should be a float, found %s', type(v))

def assert_float_eq(gold_float, guess_float, prob):
  if gold_float != guess_float:
    die(prob, 'Value should be %f, found %f', (gold_float, guess_float))

def assert_float_range(float_range, v, prob):
  if v < float_range[0] or v > float_range[1]:
    die(prob, 'Value should be in range [%f,%f], found %f',
        (float_range[0], float_range[1], v))

def assert_str(v, prob, valid_values=None):
  if type(v) != str:
    die(prob, 'Value should be a string, found %s', type(v))
  if valid_values is not None and v not in valid_values:
    die(prob, 'Not a valid value: %s, potential values: %s', (v, valid_values))

def assert_str_eq(gold_str, guess_str, prob):
  if gold_str != guess_str:
    die(prob, 'Value should be \'%s\', found \'%s\'', (gold_str, guess_str))

def assert_str_neq(gold_str, guess_str, prob):
  if gold_str == guess_str:
    die(prob, 'Value should not be \'%s\'', gold_str)

def assert_tuple(v, tup_len, prob):
  if type(v) != tuple:
    die(prob, 'Value should be a tuple, found %s', type(v))
  if len(v) != tup_len:
    die(prob, 'Tuple len should be %d, found %d', (len(v), tup_len))

def assert_list(v, list_len, prob, valid_values=None):
  if type(v) != list:
    die(prob, 'Value should be a list, found %s', type(v))
  if list_len != None and len(v) != list_len:
    die(prob, 'List len should be %d, found %d', (list_len, len(v)))
  if valid_values != None:
    for item in v:
      if item not in valid_values:
        die(prob, 'Invalid list item: %s   Valid: %s', (item, valid_values))

def assert_dict(v, prob):
  if type(v) != dict:
    die(prob, 'Value should be a dict, found %s', type(v))

def assert_dict_key(v, key, prob):
  assert_dict(v, prob)
  if key not in v:
    die(prob, 'Dict should have key \'%s\'', key)

def assert_prob_dict(v, prob):
  dsum = 0.0
  for k,value in v.items():
    if type(value) != float:
      die(prob, 'Dict key %s should have float value, found %s', (k, type(value)))
    if value < 0.0 or value > 1.0:
      die(prob, 'Dict value should be in [0,1]: %f', value)
    dsum += value
  if abs(dsum - 1.0) > 0.000001:
    die(prob, 'Dict should be probabilities, but instead sum to %f', dsum)

def assert_file(v, prob):
  if not os.path.exists(v):
    die(prob, 'File should exist: %s', v)

