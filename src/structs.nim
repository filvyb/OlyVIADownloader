type
  Queue* = ref object of RootObj
    ## Simple representation of System.Collections.Queue
    head*: int32
    tail*: int32
    size*: int32
    growFactor*: int32
    version*: int32
  
  Int32Queue* = ref object of Queue
    data*: seq[int32]

  SqlResultSet* = object
    columns*: seq[string]
    rows*: seq[seq[string]]
