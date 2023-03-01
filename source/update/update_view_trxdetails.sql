UPDATE intuipos_mexico.view_trxdetails AS f 
SET 
    bitransactionid = src.bitransactionid, 
    pointofsaleid = src.pointofsaleid, 
    transactiontypeid = src.transactiontypeid, 
    transactiontypename = src.transactiontypename, 
    itemdiscountrate = src.itemdiscountrate, 
    documentid = src.documentid, 
    externaldocumentid = src.externaldocumentid, 
    vobservations = src.vobservations, 
    dtduedate = src.dtduedate, 
    transactioninventorydate = src.transactioninventorydate, 
    invdocstatusid = src.invdocstatusid, 
    invtrxstatus = src.invtrxstatus, 
    bitransactiondetailid = src.bitransactiondetailid, 
    itemid = src.itemid, 
    itemcost = src.itemcost, 
    quantity = src.quantity, 
    totalitemrowcost = src.totalitemrowcost, 
    measureunitid_detail = src.measureunitid_detail, 
    measureunitid_inventory = src.measureunitid_inventory,
    measureunitname_indventory = src.measureunitname_indventory,
    measureunitid_recipe = src.measureunitid_recipe, 
    measureunitname_recipe = src.measureunitname_recipe, 
    tidetailstatusid = src.tidetailstatusid, 
    warehouseid = src.warehouseid, 
    warehousename = src.warehousename, 
    dtdate = src.dtdate, 
    inventoryperiod = src.inventoryperiod, 
    timeasureunitid_purchase = src.timeasureunitid_purchase, 
    dquantity_purchase = src.dquantity_purchase, 
    dfactor = src.dfactor, 
    bitransactionorderdetailid = src.bitransactionorderdetailid, 
    taxid = src.taxid, taxrate = src.taxrate, 
    taxtotal = src.taxtotal, 
    tax2id = src.tax2id, 
    tax2rate = src.tax2rate, 
    tax2total = src.tax2total, 
    itemname = src.itemname, 
    groupid = src.groupid, 
    groupname = src.groupname, 
    subgroupid = src.subgroupid, 
    subgroupname = src.subgroupname, 
    supplierid = src.supplierid, 
    suppliername = src.suppliername, 
    supplierlastname = src.supplierlastname, 
    supplierphonenumber = src.supplierphonenumber, 
    supplieraddress = src.supplieraddress, 
    suppliercontactname = src.suppliercontactname, 
    supplierdocumentid = src.supplierdocumentid, 
    isupplierid_ondetail = src.isupplierid_ondetail, 
    suppliername_ondetail = src.suppliername_ondetail, 
    totalitemrowcost_cierre = src.totalitemrowcost_cierre, 
    t_tranx_dtlastupdate = src.t_tranx_dtlastupdate, 
    t_trand_dtlastupdate = src.t_trand_dtlastupdate,
    last_updated_at = GETDATE()
FROM
    intuipos_mexico.view_trxdetails AS mmss
        INNER JOIN
    intuipos_mexico.view_trxdetails__stg AS src
        ON mmss.bitransactiondetailid = src.bitransactiondetailid
        AND mmss.bitransactionid = src.bitransactionid
WHERE 
	mmss.bitransactionid = f.bitransactionid
    AND mmss.bitransactiondetailid = F.bitransactiondetailid