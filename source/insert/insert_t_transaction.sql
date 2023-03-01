INSERT INTO intuipos_mexico.t_transaction(
SELECT 
    bitransactionid, titransactiontypeid, vterminalid, mtotalamount, tistatusid, 
    tideliverytypeid, vobservations, titableid, biemployeeid, bidocumentid, ipax, 
    biclientid, nombreenpedido, iwarehouseid, tireplicationstatusid, ipuntodeventaid, 
    ipuntodeventaid_persona, isupplierid, ipuntodeventaid_supplier, ipuntodeventaid_client, 
    vinvoiceprefix, ipuntodeventaid_terminal, ipuntodeventaid_furniture, isubpuntodeventaid, 
    tiplatformid, dtduedate, dtsystemdate, dtdate, vexternaldocumentid, vresolucionimpuestosid, 
    fecha_carga, dtlastupdate
)
SELECT
    s.bitransactionid, s.titransactiontypeid, s.vterminalid, s.mtotalamount, s.tistatusid, 
    s.tideliverytypeid, s.vobservations, s.titableid, s.biemployeeid, s.bidocumentid, s.ipax, 
    s.biclientid, s.nombreenpedido, s.iwarehouseid, s.tireplicationstatusid, s.ipuntodeventaid, 
    s.ipuntodeventaid_persona, s.isupplierid, s.ipuntodeventaid_supplier, s.ipuntodeventaid_client, 
    s.vinvoiceprefix, s.ipuntodeventaid_terminal, s.ipuntodeventaid_furniture, s.isubpuntodeventaid, 
    s.tiplatformid, s.dtduedate, s.dtsystemdate, s.dtdate, s.vexternaldocumentid, s.vresolucionimpuestosid, 
    GETDATE() AS fecha_carga, s.dtlastupdate
FROM
    intuipos_mexico.t_transaction__stg  s
        LEFT JOIN
    intuipos_mexico.t_transaction  AS f
        ON f.bitransactionid = s.bitransactionid
WHERE
    f.bitransactionid IS NULL;