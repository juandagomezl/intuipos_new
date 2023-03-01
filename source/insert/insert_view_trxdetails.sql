INSERT INTO intuipos_mexico.view_trxdetails(
SELECT 
    bitransactionid, pointofsaleid, transactiontypeid, transactiontypename, itemdiscountrate, 
    documentid, externaldocumentid, vobservations, dtduedate, transactioninventorydate, 
    invdocstatusid, invtrxstatus, bitransactiondetailid, itemid, itemcost, quantity, 
    totalitemrowcost, measureunitid_detail, measureunitid_inventory, measureunitname_indventory, 
    measureunitid_recipe, measureunitname_recipe, tidetailstatusid, warehouseid, warehousename, dtdate, 
    inventoryperiod, timeasureunitid_purchase, dquantity_purchase, dfactor, bitransactionorderdetailid, 
    taxid, taxrate, taxtotal, tax2id, tax2rate, tax2total, itemname, groupid, groupname, subgroupid, subgroupname, 
    supplierid, suppliername, supplierlastname, supplierphonenumber, supplieraddress, suppliercontactname, 
    supplierdocumentid, isupplierid_ondetail, suppliername_ondetail, fecha_creacion, totalitemrowcost_cierre, 
    t_tranx_dtlastupdate, t_trand_dtlastupdate
)
SELECT
    s.bitransactionid, s.pointofsaleid, s.transactiontypeid, s.transactiontypename, s.itemdiscountrate,
    s.documentid, s.externaldocumentid, s.vobservations, s.dtduedate, s.transactioninventorydate,
    s.invdocstatusid, s.invtrxstatus, s.bitransactiondetailid, s.itemid, s.itemcost, s.quantity,
    s.totalitemrowcost, s.measureunitid_detail, s.measureunitid_inventory, s.measureunitname_indventory,
    s.measureunitid_recipe, s.measureunitname_recipe, s.tidetailstatusid, s.warehouseid, s.warehousename, s.dtdate,
    s.inventoryperiod, s.timeasureunitid_purchase, s.dquantity_purchase, s.dfactor, s.bitransactionorderdetailid,
    s.taxid, s.taxrate, s.taxtotal, s.tax2id, s.tax2rate, s.tax2total, s.itemname, s.groupid, s.groupname, s.subgroupid, s.subgroupname,
    s.supplierid, s.suppliername, s.supplierlastname, s.supplierphonenumber, s.supplieraddress, s.suppliercontactname,
    s.supplierdocumentid, s.isupplierid_ondetail, s.suppliername_ondetail, GETDATE() AS fecha_creacion,
    s.totalitemrowcost_cierre, s.t_tranx_dtlastupdate, s.t_trand_dtlastupdate
FROM
    intuipos_mexico.view_trxdetails__stg  s
        LEFT JOIN
    intuipos_mexico.view_trxdetails  AS f
        ON f.bitransactiondetailid = s.bitransactiondetailid
        AND f.bitransactionid = s.bitransactionid
WHERE
    f.bitransactiondetailid IS NULL;


