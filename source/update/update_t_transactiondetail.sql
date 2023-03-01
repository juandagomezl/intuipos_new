UPDATE intuipos_mexico.t_transactiondetail AS f 
SET 
    iitemid=src.iitemid, 
    bitransactionid=src.bitransactionid,
    mitemprice=src.mitemprice,
    bifatherid=src.bifatherid,
    fdiscountpercentage=src.fdiscountpercentage,
    dtaxpercentage=src.dtaxpercentage, 
    dquantity=src.dquantity, 
    iwarehouseid=src.iwarehouseid, 
    tireplicationstatusid=src.tireplicationstatusid, 
    timeasureunitid=src.timeasureunitid, 
    dtaxpercentage2=src.dtaxpercentage2, 
    ipuntodeventaid_item=src.ipuntodeventaid_item, 
    dtdate=src.dtdate, iperiodo=src.iperiodo, 
    tidetailstatusid=src.tidetailstatusid, 
    itaxid=src.itaxid, itaxid2=src.itaxid2, 
    isupplierid=src.isupplierid, 
    ipuntodeventaid_supplier=src.ipuntodeventaid_supplier, 
    vobservations=src.vobservations,
    dtlastupdate=src.dtlastupdate
FROM
    intuipos_mexico.t_transactiondetail AS mmss
        INNER JOIN
    intuipos_mexico.t_transactiondetail__stg AS src
        ON mmss.bitransactionid = src.bitransactionid
WHERE 
	mmss.bitransactionid = f.bitransactionid