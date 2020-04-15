<?php

// Create XML Dataset from sql query list
// php fb.php "select * from adm_firmy;select * from gm_waluta_kurs" "admfirmy;CurrRate"
// php fb.php "select * from gm_zest_mag_on_day (1,34,'07.07.2013',0);select * from adm_firmy" "WhRest;admfirmy" UseTranslit > 1.xml
// first argument  = sql query list with ";" as delimiter
// second argument = output tablename list with ";" as delimiter
// third argument  = Usetranslit - to convert polish characters to latin. Can be ommited.

function polishtranslit($str)
{
// translit utf-8 (polish) -> latin
// C4 84 -> A
// C4 85 -> a
// C4 86 -> C
// C4 87 -> c
// C4 98 -> E
// C4 99 -> e
// C5 81 -> L
// C5 82 -> l
// C5 83 -> N
// C5 84 -> n
// C3 93 -> O
// C3 B3 -> o
// C5 9A -> S
// C5 9B -> s
// C5 B9 -> Z
// C5 BA -> z
// C5 BB -> Z
// C5 BC -> z
// C9 97 -> Y
// C9 98 -> y
  $utf8char  = array("\xC4\x84", "\xC4\x85", "\xC4\x86", "\xC4\x87", "\xC4\x98", "\xC4\x99", "\xC5\x81", "\xC5\x82", "\xC5\x83",
                     "\xC5\x84", "\xC3\x93", "\xC3\xB3", "\xC5\x9A", "\xC5\x9B", "\xC5\xB9", "\xC5\xBA", "\xC5\xBB", "\xC5\xBC",
                     "\xC9\x97", "\xCB\x98", "\xC2\xA8", "\xC2\xA9", "\xC5\xA5", "\xC5\xA6", "\xC4\xBE");
  $replace   = array ("A",       "a",        "C",        "c",        "E",        "e",        "L",        "l",        "N",
                      "n",       "O",        "o",        "S",        "s",        "Z",        "z",        "Z",        "z",
                      "Y",       "y",        "E",        "e",        "L",        "l",        "c");

  $str = str_replace($utf8char, $replace, $str);
  return $str;
}

function replacexml ($str)
{
// change some symbols in strings to create corrent xml file
// &<> -> &amp;&lt;&gt; 
  $xmlchar  = array ('&',     '<',    '>', "\x00");
  $replace  = array ('&amp;', '&lt;', '&gt;', 'l');

  $str = str_replace($xmlchar, $replace, $str);
  return $str;
}

$stmtlist = $argv[1];
$stmt = explode(";", $stmtlist);
$tablenamelist = $argv[2];
$tablename = explode(";", $tablenamelist);
$stmtcount = count ($stmt);
$tablecount = count ($tablename);

if ($stmtlist == "")
{
  echo "Empty sql query\n";
  return;
}
if ($tablenamelist == "")
{
  echo "Empty tablenameList\n";
  return;
}
if ($stmtcount != $tablecount )
{
  echo "sql statement list count differs from tablename list count\n";
  return;
}
$DatasetName = "ProDataSet";
$Usetranslit = "no";
if ($argc > 3) {$Usetranslit = $argv[3];}


// load connection parameters from fb.cfg file
$cfgXML = "fb.cfg";
$doc = new DOMDocument();
$doc->load($cfgXML);
$tags = $doc->getElementsByTagName('db');
foreach ($tags as $tag) {$host = $tag->nodeValue;}
$tags = $doc->getElementsByTagName('user');
foreach ($tags as $tag) {$user = $tag->nodeValue;}
$tags = $doc->getElementsByTagName('password');
foreach ($tags as $tag) {$password = $tag->nodeValue;}
$dbh = ibase_connect($host, $user, $password, 'UTF8');


for ($j = 0; $j < $stmtcount; $j++) 
{
  $sth[$j] = ibase_query($dbh, $stmt[$j]);
  $coln[$j] = ibase_num_fields($sth[$j]);
}

echo "<?xml version=\"1.0\"?>\n";
echo "<", $DatasetName, " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n";
echo "  <xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"\" xmlns:prodata=\"urn:schemas-progress-com:xml-prodata:0001\">\n";
echo "    <xsd:element name=\"", $DatasetName, "\" prodata:proDataSet=\"true\">\n";
echo "      <xsd:complexType>\n";
echo "        <xsd:sequence>\n";


for ($j = 0; $j < $stmtcount; $j++)
{ 
echo "          <xsd:element name=\"", $tablename[$j], "\" minOccurs=\"0\" maxOccurs=\"unbounded\">\n";
echo "            <xsd:complexType>\n";
echo "              <xsd:sequence>\n";

  for ($i = 0; $i < $coln[$j]; $i++) 
  {
    $col_info = ibase_field_info($sth[$j], $i);    
    $fieldtype = $col_info['type'];
    $fieldname = $col_info['alias'];
    $fieldlabel = $col_info['alias'];

    $progresstype = $fieldtype;
    if ($fieldtype == "BLOB") {continue;}
    if ($fieldtype == "VARCHAR") {$progresstype = "string";}
    if ($fieldtype == "CHAR") {$progresstype = "string";}
    if (substr( $fieldtype, 0, strlen( "CHAR" ) ) == "CHAR") {$progresstype = "string";}
    if ($fieldtype == "INTEGER") {$progresstype = "int";}
    if ($fieldtype == "SMALLINT") {$progresstype = "int";}
    if ($fieldtype == "INT64") {$progresstype = "long";}
    if (substr( $fieldtype, 0, strlen( "NUMERIC" ) ) == "NUMERIC") {$progresstype = "decimal";}
    if (substr( $fieldtype, 0, strlen( "DECIMAL" ) ) == "DECIMAL") {$progresstype = "decimal";}
    if ($fieldtype == "FLOAT") {$progresstype = "decimal";}
    if ($fieldtype == "DOUBLE") {$progresstype = "decimal";}
    if ($fieldtype == "BOOLEAN") {$progresstype = "boolean";}
    if ($fieldtype == "DATE")    {$progresstype = "date";}
    if ($fieldtype == "TIMESTAMP")    {$progresstype = "dateTime";}

echo "                <xsd:element name=\"", $fieldname, "\" type=\"xsd:", $progresstype, "\" nillable=\"true\" prodata:label=\"", $fieldlabel, "\"/>\n";
  }
echo "              </xsd:sequence>\n";
echo "            </xsd:complexType>\n";
echo "          </xsd:element>\n";
}
echo "        </xsd:sequence>\n";
echo "      </xsd:complexType>\n";
echo "    </xsd:element>\n";
echo "  </xsd:schema>\n";

for ($j = 0; $j < $stmtcount; $j++)
{
  while ($row = ibase_fetch_object($sth[$j])) 
  {
echo "  <", $tablename[$j], ">\n";
    for ($i = 0; $i < $coln[$j]; $i++) 
    {
      $col_info = ibase_field_info($sth[$j], $i);    
      $fieldname = $col_info['alias'];
      $value = $row->$fieldname;

      $fieldtype = $col_info['type'];   
      $progresstype = $fieldtype;
      if ($fieldtype == "BLOB") {continue;}
      if ($fieldtype == "VARCHAR") {$progresstype = "string";}
      if ($fieldtype == "CHAR") {$progresstype = "string";}
      if (substr( $fieldtype, 0, strlen( "CHAR" ) ) == "CHAR") {$progresstype = "string";}
      if ($Usetranslit == "UseTranslit")
      {
        if ($progresstype == "string") {$value = polishtranslit ($value);}
      }
      if ($progresstype == "string") {$value = replacexml ($value);}
      if (substr( $fieldtype, 0, strlen( "NUMERIC" ) ) == "NUMERIC") {$progresstype = "decimal";}
      if (substr( $fieldtype, 0, strlen( "DECIMAL" ) ) == "DECIMAL") {$progresstype = "decimal";}
      if ($fieldtype == "FLOAT") {$progresstype = "decimal";}
      if ($fieldtype == "DOUBLE") {$progresstype = "decimal";}
      if ($progresstype == "decimal")
      {
echo "    <", $fieldname, ">", sprintf("%f", $value), "</", $fieldname, ">\n";
      }
      else 
      {
echo "    <", $fieldname, ">", $value, "</", $fieldname, ">\n";
      }
    }
echo "  </", $tablename[$j], ">\n";
  }
  ibase_free_result($sth[$j]);
}
echo "</", $DatasetName, ">\n";
ibase_close($dbh);
?>
