define input  parameter ModuleParams as character.                
define output parameter ResultFile   as character initial "".     
define output parameter ResultString as character initial "OK".  

define variable sqlquery as character.
define variable tablename as character.
run src/system/getmpfi2.p ("xml").
ResultFile = RETURN-VALUE.

sqlquery = ENTRY(1,ModuleParams,";").
if num-entries (ModuleParams, ";") > 1 then
  tablename = ENTRY(2,ModuleParams,";").

if tablename <> "" then
  os-command value ('php fb.php ' + '"' + sqlquery + '"' + ' ' + tablename + ' > ' + ResultFile).
else
  os-command value ('php fb.php ' + '"' + sqlquery + '"' + ' > ' + ResultFile).

define variable line as character.
input from value (ResultFile).
repeat:
import unformatted line.
if line = "" then next.
if line <> '<?xml version="1.0"?>' then
do:
  ResultString = "ERROR:" + line.
end.
leave.
end.
input close.

if ResultString begins "ERROR" then
do:
  os-delete value(ResultFile).
  ResultFile = "".
end.
