CREATE TABLE intuipos_mexico.view_trxdetails__stg
(
	bitransactionid BIGINT   ENCODE az64
	,pointofsaleid BIGINT   ENCODE az64
	,transactiontypeid BIGINT   ENCODE az64
	,transactiontypename VARCHAR(256)   ENCODE lzo
	,itemdiscountrate NUMERIC(20,4)   ENCODE az64
	,documentid NUMERIC(20,4)   ENCODE az64
	,externaldocumentid VARCHAR(256)   ENCODE lzo
	,vobservations VARCHAR(256)   ENCODE lzo
	,dtduedate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,transactioninventorydate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invdocstatusid NUMERIC(20,4)   ENCODE az64
	,invtrxstatus VARCHAR(256)   ENCODE lzo
	,bitransactiondetailid NUMERIC(20,4)   ENCODE az64
	,itemid NUMERIC(20,4)   ENCODE az64
	,itemcost NUMERIC(20,4)   ENCODE az64
	,quantity NUMERIC(20,4)   ENCODE az64
	,totalitemrowcost NUMERIC(20,4)   ENCODE az64
	,measureunitid_detail NUMERIC(20,4)   ENCODE az64
	,measureunitid_inventory NUMERIC(20,4)   ENCODE az64
	,measureunitname_indventory VARCHAR(256)   ENCODE lzo
	,measureunitid_recipe NUMERIC(20,4)   ENCODE az64
	,measureunitname_recipe VARCHAR(256)   ENCODE lzo
	,tidetailstatusid NUMERIC(20,4)   ENCODE az64
	,warehouseid BIGINT   ENCODE az64
	,warehousename VARCHAR(256)   ENCODE lzo
	,dtdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,inventoryperiod BIGINT   ENCODE az64
	,timeasureunitid_purchase BIGINT   ENCODE az64
	,dquantity_purchase NUMERIC(20,4)   ENCODE az64
	,dfactor BIGINT   ENCODE az64
	,bitransactionorderdetailid BIGINT   ENCODE az64
	,taxid BIGINT   ENCODE az64
	,taxrate NUMERIC(20,7)   ENCODE az64
	,taxtotal NUMERIC(20,7)   ENCODE az64
	,tax2id BIGINT   ENCODE az64
	,tax2rate NUMERIC(20,4)   ENCODE az64
	,tax2total NUMERIC(20,4)   ENCODE az64
	,itemname VARCHAR(256)   ENCODE lzo
	,groupid BIGINT   ENCODE az64
	,groupname VARCHAR(256)   ENCODE lzo
	,subgroupid BIGINT   ENCODE az64
	,subgroupname VARCHAR(256)   ENCODE lzo
	,supplierid BIGINT   ENCODE az64
	,suppliername VARCHAR(256)   ENCODE lzo
	,supplierlastname VARCHAR(256)   ENCODE lzo
	,supplierphonenumber VARCHAR(256)   ENCODE lzo
	,supplieraddress VARCHAR(256)   ENCODE lzo
	,suppliercontactname VARCHAR(256)   ENCODE lzo
	,supplierdocumentid VARCHAR(256)   ENCODE lzo
	,isupplierid_ondetail BIGINT   ENCODE az64
	,suppliername_ondetail VARCHAR(256)   ENCODE lzo
	,totalitemrowcost_cierre NUMERIC(20,4)   ENCODE az64
	,t_tranx_dtlastupdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,t_trand_dtlastupdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
;