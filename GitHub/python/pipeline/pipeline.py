#/usr/bin/python
# -*- coding: utf-8 -*-
  
"""
:Functional Test  -- that does some Pipeline functions
===================================
 
.. Functional:: pipeline
   :platform: Unix, Windows
   :synopsis: pipeline document snippets that match a query.
.. moduleauthor:: Suraj Rajendran
 
  
Requirements::
    1.  You will need to install the pyhton library to run this code.
       
        See https://www.python.org/downloads/
         
      
 
"""
# python 3.6
# Author : Suraj Rajendran v0.1.
# The imperative loop below performs transformations on dictionaries that hold the name, incorrect Birth and active status of some bands
# ----------------------------------------------------------------------------------
# Create the group object

from functools import reduce
import unittest 
from unittest.mock import patch
import json
import doctest

bands = [{'name': 'Suraj', 'Birth': 'UK', 'active': False},
         {'name': 'Rajendran', 'Birth': 'Germany', 'active': False},
         {'name': 'Agassis', 'Birth': 'Spain', 'active': True}]
expected = [{'name': 'Suraj', 'Birth': 'UK', 'active': False},
         {'name': 'Rajendran', 'Birth': 'Germany', 'active': False},
         {'name': 'Agassis', 'Birth': 'Spain', 'active': True}]
dump = json.dumps(bands, sort_keys=True, indent=2)

# This function is itertive , and NOT reuseable, hard to test and hard to parallelize
def format_bands(bands):
    assert expected == bands
    AssertionError
    for band in bands:
        band['Birth'] = 'India'
        band['name'] = band['name'].replace('.', '')
        band['name'] = band['name'].title()

format_bands(bands)

print (dump)

# Any mutating operations (here string) copy the value, change it and pass back the changed copy
def assoc(_d, key, value):
    from copy import deepcopy
    d = deepcopy(_d)
    d[key] = value
    return d

# Turn iterations of lists into maps and reduces.
# This function is itertive , and  reuseable, EASY to test and parallelizable
# Like a race() Break code into functions. Make those functions functional. Turn a loop that repeats a process into a recursion
# Tail call optimisation is a programming language feature. Each time a function recurses, a new stack frame is created.
# Languages like Python that do not have tail call optimisation generally limit the number of times a function may recurse to some number in the thousands. 
# In the case of the race() function, there are only five time steps, so it is safe.
def pipeline(data, fns):
	return reduce(lambda a, x: list(map(x, a)),
	  fns,
	  data)

# call() is a higher order function. A higher order function takes a function as an argument, or returns a function. Or, like call(), it does both.
# call()  used to generate pipeline functions. Functional programming is partly about building up a library of generic, reusable, composable functions.
# apply_fn() looks very similar to the three transformation functions. It takes a record (a band). It looks up the value at record[key]. 
# apply_fn() calls fn on that value. It assigns the result back to a copy of the record. It returns the copy.
def call(fn, key):
    def apply_fn(record):
        return assoc(record, key, fn(record.get(key)))
    return apply_fn

# extract_name_and_birth(set_india_as_Birth) is not written as a generic function 
# series of non-generic functions
set_india_as_Birth = call(lambda x: 'India', 'Birth')
strip_punctuation_from_name = call(lambda x: x.replace('.', ''), 'name')
capitalize_names = call(str.title, 'name')
print (pipeline(bands, [set_india_as_Birth,
                            strip_punctuation_from_name,
                            capitalize_names]))

# extract_name_and_birth(set_india_as_Birth) written as a generic function called replace() ,replace takes a list of keys to extract from each record
def replace(keys):
    def replace_fn(record):
        return reduce(lambda a, x: assoc(a, x, record[x]),
                      keys,
                      {})
    return replace_fn

print (pipeline(bands, [call(lambda x: 'India', 'Birth'),
                            call(lambda x: x.replace('.', ''), 'name'),
                            call(str.title, 'name'),
                            replace(['name', 'Birth'])]))
                            
if __name__ == "__main__": 
    doctest.testmod()
# => [{'name': 'Sunset Rubdown', 'active': False, 'Birth': 'India'},
#     {'name': 'Women', 'active': False, 'Birth': 'India' },
#     {'name': 'A Silver Mt Zion', 'active': True, 'Birth': 'India'}]
