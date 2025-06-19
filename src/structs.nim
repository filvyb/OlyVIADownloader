import sequtils, strutils

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

proc `$`*(resultSet: SqlResultSet): string =
  ## Convert the SQL result set to a formatted table string
  if resultSet.columns.len == 0:
    return "No results"
  
  var lines: seq[string] = @[]
  
  # Calculate column widths
  var widths: seq[int] = @[]
  for i, col in resultSet.columns:
    var maxWidth = col.len
    for row in resultSet.rows:
      if i < row.len and row[i].len > maxWidth:
        maxWidth = row[i].len
    widths.add(maxWidth + 2)
  
  # Add header
  lines.add("┌" & widths.mapIt("─".repeat(it)).join("┬") & "┐")
  var headerLine = "│"
  for i, col in resultSet.columns:
    headerLine.add(" " & col.alignLeft(widths[i] - 2) & " │")
  lines.add(headerLine)
  lines.add("├" & widths.mapIt("─".repeat(it)).join("┼") & "┤")
  
  # Add rows
  for row in resultSet.rows:
    var rowLine = "│"
    for i, val in row:
      if i < widths.len:
        rowLine.add(" " & val.alignLeft(widths[i] - 2) & " │")
    lines.add(rowLine)
  
  lines.add("└" & widths.mapIt("─".repeat(it)).join("┴") & "┘")
  
  return lines.join("\n")
