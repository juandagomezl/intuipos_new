UPDATE intuipos_mexico.t_transaction AS f 
SET 
    bitransactionid=src.bitransactionid, 
    titransactiontypeid=src.titransactiontypeid, 
    vterminalid=src.vterminalid, 
    mtotalamount=src.mtotalamount, 
    tistatusid=src.tistatusid, 
    tideliverytypeid=src.tideliverytypeid, 
    vobservations=src.vobservations, 
    titableid=src.titableid, 
    biemployeeid=src.biemployeeid, 
    bidocumentid=src.bidocumentid, 
    ipax=src.ipax, biclientid=src.biclientid, 
    nombreenpedido=src.nombreenpedido, 
    iwarehouseid=src.iwarehouseid, 
    tireplicationstatusid=src.tireplicationstatusid, 
    ipuntodeventaid=src.ipuntodeventaid, 
    ipuntodeventaid_persona=src.ipuntodeventaid_persona, 
    isupplierid=src.isupplierid, 
    ipuntodeventaid_supplier=src.ipuntodeventaid_supplier, 
    ipuntodeventaid_client=src.ipuntodeventaid_client, 
    vinvoiceprefix=src.vinvoiceprefix, 
    ipuntodeventaid_terminal=src.ipuntodeventaid_terminal, 
    ipuntodeventaid_furniture=src.ipuntodeventaid_furniture, 
    isubpuntodeventaid=src.isubpuntodeventaid, 
    tiplatformid=src.tiplatformid, 
    dtduedate=src.dtduedate, 
    dtsystemdate=src.dtsystemdate, 
    dtdate=src.dtdate, 
    vexternaldocumentid=src.vexternaldocumentid, 
    vresolucionimpuestosid=src.vresolucionimpuestosid, 
    dtlastupdate=src.dtlastupdate 
FROM
    intuipos_mexico.t_transaction AS mmss
        INNER JOIN
    intuipos_mexico.t_transaction__stg AS src
        ON mmss.bitransactionid = src.bitransactionid
WHERE 
	mmss.bitransactionid = f.bitransactionid