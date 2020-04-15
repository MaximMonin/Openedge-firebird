define variable FileName     as character.
define variable ResultString as character.
define variable rid-country  as integer.

define buffer clients2 for clients.

/* загружаем данные */
Run src/transfer/fb/sqlds.p ("select * from kontrah_typ;select KOD,NAZWA,NIP,NIP_EU,SHORTNAME,TEL1,TEL2,Fax,WWW,Mail,KODPOCZT,REGON,POCZTA,ULICA,TYP from kontrah;" 
    + "select k.KOD, k.NAZWA, g.DATAWYST, g.TERM_PLAT from gm_nagwz g inner join kontrah k ON k.kod = g.KTRH order by k.KOD, g.DATAWYST" 
    + "#kontrah_typ;kontrah;otsr#UseTranslit",
    output filename, output ResultString).
/*  */
if ResultString <> "OK"
then return.

{config.i}

/* Считываем текущие таблицы */
define temp-table t_clients     NO-UNDO LIKE-SEQUENTIAL clients.
define temp-table t_client-type NO-UNDO LIKE-SEQUENTIAL client-type.

DEFINE DATASET cldata for t_clients, t_client-type.

define variable hDS    as handle.
define variable hDSnew as handle.

DEFINE QUERY q_clients FOR clients.
QUERY q_clients:QUERY-PREPARE("FOR EACH clients NO-LOCK").

DEFINE QUERY q_client-type FOR client-type.
QUERY q_client-type:QUERY-PREPARE("FOR EACH client-type NO-LOCK").

DEFINE DATA-SOURCE ds_clients     FOR QUERY q_clients.
DEFINE DATA-SOURCE ds_client-type FOR QUERY q_client-type.

BUFFER     t_clients:ATTACH-DATA-SOURCE (DATA-SOURCE ds_clients:HANDLE ).
BUFFER t_client-type:ATTACH-DATA-SOURCE (DATA-SOURCE ds_client-type:HANDLE).

hDS = DATASET cldata:HANDLE.
hDS:FILL ().

/* Считываем пришедшие таблицы */
run src/transfer/XmlReadDataset.p (FileName, OUTPUT DATASET-HANDLE hDSnew).

/* Делаем сравнение и замену по всем таблицам */
define variable tn      as character.
define variable hBuffer as handle.
define variable qBuf    as handle.
define variable qBuf2   as handle.
define variable buf2    as handle.
define variable buf3    as handle.
define variable str     as character.
define variable i       as integer.
define variable rid-upobject as integer.
define variable kod     as integer.
define variable nazwa   as character.
define variable lastclient as integer.
define variable lastterm as integer.
define variable termpl as integer.

lastclient = 0.
lastterm = 0.

run src/kernel/get_root.p(2, output rid-upobject).

DO i = 1 TO hDSnew:NUM-BUFFERS:
  hBuffer = hDSnew:GET-BUFFER-HANDLE(i).

  tn = hBuffer:NAME.
  CREATE QUERY qBuf.
  qBuf:SET-BUFFERS(hBuffer).
  qBuf:QUERY-PREPARE("FOR EACH " + tn + " NO-LOCK").
  qBuf:QUERY-OPEN().
  qBuf:GET-FIRST().
  repeat:
    IF NOT hBuffer:AVAILABLE then LEAVE.

    if tn = "kontrah_typ" then do:
      kod = hBuffer:BUFFER-FIELD("KOD"):BUFFER-VALUE NO-ERROR. 
      find first t_client-type where t_client-type.id-clt = kod no-lock no-error.
      if not available t_client-type then
      do:
        create client-type.
        assign 
          client-type.id-clt    = kod
          client-type.rid-anobject = kod.
      end.
      else do:
        find first client-type where client-type.id-clt = kod no-error. 
      end.
      client-type.name-clt = hBuffer:BUFFER-FIELD("NAZWA"):BUFFER-VALUE NO-ERROR. 
    end.

    if tn = "kontrah" then
    do:
      kod = hBuffer:BUFFER-FIELD("KOD"):BUFFER-VALUE NO-ERROR.

      find first t_clients where t_clients.Id-client = kod no-lock no-error.
      if not available t_clients then
      do:
        create clients.
        assign clients.Id-client = kod. 

        find first anobject where anobject.id-anobject = clients.id-client and
          anobject.rid-upobject  = rid-upobject NO-LOCK NO-ERROR.
        if not available anobject then
        do:
          create anobject.
          assign
            anobject.id-anobject   = clients.id-client
            anobject.name-anobject = hBuffer:BUFFER-FIELD("NAZWA"):BUFFER-VALUE + " (" + STRING(kod) + ")"
            anobject.rid-upobject  = rid-upobject.
        end.
        clients.rid-anobject   = anobject.rid-anobject.
      end.
      else do:
        find first clients where clients.Id-client = kod NO-ERROR. 
      end.
      nazwa = hBuffer:BUFFER-FIELD("NAZWA"):BUFFER-VALUE + " (" + STRING(kod) + ")".

      clients.Name-client = nazwa. 

      clients.Zkpo        = hBuffer:BUFFER-FIELD("REGON"):BUFFER-VALUE NO-ERROR. 
      clients.Phone       = hBuffer:BUFFER-FIELD("TEL1"):BUFFER-VALUE NO-ERROR. 
      clients.Phone2      = hBuffer:BUFFER-FIELD("TEL2"):BUFFER-VALUE NO-ERROR. 
      clients.Fax         = hBuffer:BUFFER-FIELD("Fax" ):BUFFER-VALUE NO-ERROR. 
      clients.Web         = hBuffer:BUFFER-FIELD("WWW" ):BUFFER-VALUE NO-ERROR. 
      clients.Email       = hBuffer:BUFFER-FIELD("Mail"):BUFFER-VALUE NO-ERROR. 

      /* Заполняем параметр 126 КПП/ЕДРИСИ */
      /* Только для тех у кого задан REGON, только для не польских компаний. */
      /* По новому правилу параметр 126 храниться не в поле NIP, а в поле SHORTNAME */
      define variable nip as character.
      define variable nipeu as character.
/*      nip = hBuffer:BUFFER-FIELD("NIP"):BUFFER-VALUE NO-ERROR. */
      nip = hBuffer:BUFFER-FIELD("SHORTNAME"):BUFFER-VALUE NO-ERROR. 

      nipeu = hBuffer:BUFFER-FIELD("NIP_EU"):BUFFER-VALUE NO-ERROR. 
      if clients.zkpo = "" then
        nip = "".
      if nipeu begins "PL" then nip = "".

      find first client-param where client-param.id-param = 126 NO-LOCK NO-ERROR.
      if available client-param then
      do:
        find first client-pvalue where client-pvalue.rid-param = client-param.rid-param and
                                       client-pvalue.rid-clients = clients.rid-clients NO-ERROR.
        if nip = "" and available client-pvalue then
          delete client-pvalue.
        if nip <> "" then
        do:
          if not available client-pvalue then
          do:
            create client-pvalue.
            assign
            client-pvalue.rid-param = client-param.rid-param
            client-pvalue.rid-clients = clients.rid-clients.
            client-pvalue.val-date = date ("01/01/2013").
          end.
          client-pvalue.val = nip.
        end.
      end.

      /* Проставим признаки внутренних контрагентов как параметр 77 = "Да" */
      find first client-param where client-param.id-param = 77 NO-LOCK NO-ERROR.
      if available client-param then
      do:
        find first client-pvalue where client-pvalue.rid-param = client-param.rid-param and
                                       client-pvalue.rid-clients = clients.rid-clients NO-ERROR.

&IF {&OblikEnt} = 'oblikpl01' &THEN
        if kod = 3 or kod = 56 or kod = 134 or kod = 303 or kod = 451 or kod = 452 or kod = 618 or kod = 641 or kod = 807 or 
           kod = 1013 or kod = 1065 or kod = 1112 or kod = 1209 or kod = 1210 OR kod = 1536 or kod = 1661 or kod = 1712 then
&ENDIF
&IF {&OblikEnt} = 'oblikpl02' &THEN
        if TRIM(clients.zkpo) <> "" or (kod = 3 or kod = 56 or kod = 839 or kod = 989 or kod = 1026 or kod = 1035 or kod = 1043 or
           kod = 1049 or kod = 1063 or kod = 1089 or kod = 1205 or kod = 1206 or kod = 1207 or kod = 1208 or kod = 1209 or kod = 1312 or
           kod = 1322 or kod = 1381 or kod = 1382 or kod = 1676 or kod = 1678 or kod = 1770 or kod = 1771 or kod = 1807 or kod = 1925 or kod = 2150 or kod = 2213
           or kod = 134)
        then
&ENDIF
&IF {&OblikEnt} = 'oblikpl03' &THEN
        if kod = 3 or kod = 56 or kod = 134 or kod = 303 or kod = 451 or kod = 452 or kod = 618 or kod = 641 or kod = 807 or 
           kod = 1013 or kod = 1065 or kod = 1112 or kod = 1209 or kod = 1210 or kod = 1536 or kod = 1712 or kod = 1775 then
&ENDIF
        do:
          if not available client-pvalue then
          do:
            create client-pvalue.
            assign
            client-pvalue.rid-param = client-param.rid-param
            client-pvalue.rid-clients = clients.rid-clients.
            client-pvalue.val-date = date ("01/01/2013").
          end.
          client-pvalue.val = "Да".
        end.
        else do:
          if available client-pvalue then
            delete client-pvalue.
        end.
      end.

      str =       hBuffer:BUFFER-FIELD("KODPOCZT"):BUFFER-VALUE NO-ERROR. 
      str = str + " " + hBuffer:BUFFER-FIELD("POCZTA"):BUFFER-VALUE NO-ERROR. 
      str = str + " " + hBuffer:BUFFER-FIELD("ULICA"):BUFFER-VALUE NO-ERROR. 
      clients.Place = str.

      find first client-type where client-type.id-clt = hBuffer:BUFFER-FIELD("TYP"):BUFFER-VALUE NO-ERROR. 
      if available client-type then
        clients.rid-clt = client-type.rid-clt.
      find first anobject of clients NO-ERROR.
      if available anobject then
        anobject.name-anobject = nazwa.
    end.
    if tn = "otsr" then /* Отсрочка платежа по последним отгрузкам */
    do:
      kod = hBuffer:BUFFER-FIELD("KOD"):BUFFER-VALUE NO-ERROR.
      termpl = hBuffer:BUFFER-FIELD("TERM_PLAT"):BUFFER-VALUE NO-ERROR.
      if not (kod = lastclient and termpl = lastterm) then
      do:
        find first clients where clients.Id-client = kod NO-LOCK NO-ERROR. 
        find first client-param where client-param.id-param = 44 NO-LOCK NO-ERROR.
        if available client-param and available clients then
        do:
          find first client-pvalue where client-pvalue.rid-param = client-param.rid-param and
                                         client-pvalue.rid-clients = clients.rid-clients and 
                                         client-pvalue.val-date = hBuffer:BUFFER-FIELD("DATAWYST"):BUFFER-VALUE NO-ERROR.
          if not available client-pvalue then
          do:
            create client-pvalue.
            assign
            client-pvalue.rid-param = client-param.rid-param
            client-pvalue.rid-clients = clients.rid-clients.
            client-pvalue.val-date = date (hBuffer:BUFFER-FIELD("DATAWYST"):BUFFER-VALUE).
          end.
          client-pvalue.val = STRING(hBuffer:BUFFER-FIELD("TERM_PLAT"):BUFFER-VALUE).
        end.
      end.
      lastclient = kod.
      lastterm = termpl.
    end.

   qBuf:GET-NEXT().
  end.
  qBuf:QUERY-CLOSE().
  delete object qBuf.
END.

RETURN "OK".

procedure add-anobject:
  def input parameter p-kod    as int.
  def input parameter p-rid-up as int.
  def input parameter p-nazwa  as char.

  find first anobject where
    anobject.id-anobject = p-kod and
    anobject.rid-upobject = p-rid-up no-error.

  if not available anobject then
  do:
        CREATE anobject.
        assign 
          anobject.id-anobject   = p-kod
          anobject.rid-upobject  = p-rid-up.
  end.
  anobject.name-anobject = p-nazwa.

  return string( anobject.rid-anobject).

end. /* proc */

