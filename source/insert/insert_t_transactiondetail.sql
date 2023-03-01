INSERT INTO intuipos_mexico.t_transactiondetail(
SELECT 
    bitransactiondetailid, iitemid, bitransactionid, mitemprice, bifatherid, fdiscountpercentage, 
    dtaxpercentage, dquantity, iwarehouseid, tireplicationstatusid, timeasureunitid, dtaxpercentage2, 
    ipuntodeventaid_item, dtdate, iperiodo, tidetailstatusid, itaxid, itaxid2, isupplierid, 
    ipuntodeventaid_supplier, vobservations, ipuntodeventaid, 
    fecha_carga, dtlastupdate
)
SELECT
    s.bitransactiondetailid, s.iitemid, s.bitransactionid, s.mitemprice, s.bifatherid, s.fdiscountpercentage, 
    s.dtaxpercentage, s.dquantity, s.iwarehouseid, s.tireplicationstatusid, s.timeasureunitid, s.dtaxpercentage2, 
    s.ipuntodeventaid_item, s.dtdate, s.iperiodo, s.tidetailstatusid, s.itaxid, s.itaxid2, s.isupplierid, 
    s.ipuntodeventaid_supplier, s.vobservations, s.ipuntodeventaid,
    GETDATE() AS fecha_carga, s.dtlastupdate
FROM
    intuipos_mexico.t_transactiondetail__stg  s
        LEFT JOIN
    intuipos_mexico.t_transactiondetail  AS f
        ON f.bitransactiondetailid = s.bitransactiondetailid
WHERE
    f.bitransactiondetailid IS NULL;