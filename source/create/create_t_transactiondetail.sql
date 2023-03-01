CREATE TABLE  intuipos_mexico.t_transactiondetail__stg
(
	bitransactiondetailid BIGINT NOT NULL  ENCODE az64
	,iitemid BIGINT   ENCODE az64
	,bitransactionid BIGINT   ENCODE az64
	,mitemprice NUMERIC(18,4)   ENCODE az64
	,bifatherid BIGINT   ENCODE az64
	,fdiscountpercentage DOUBLE PRECISION   ENCODE RAW
	,dtaxpercentage NUMERIC(7,4)   ENCODE az64
	,dquantity NUMERIC(18,5)   ENCODE az64
	,iwarehouseid BIGINT   ENCODE az64
	,tireplicationstatusid BIGINT   ENCODE az64
	,timeasureunitid BIGINT   ENCODE az64
	,dtaxpercentage2 NUMERIC(7,4)   ENCODE az64
	,ipuntodeventaid_item BIGINT   ENCODE az64
	,dtdate TIMESTAMP WITHOUT TIME ZONE   ENCODE RAW
	,iperiodo BIGINT   ENCODE az64
	,tidetailstatusid BIGINT   ENCODE az64
	,itaxid BIGINT   ENCODE az64
	,itaxid2 BIGINT   ENCODE az64
	,isupplierid BIGINT   ENCODE az64
	,ipuntodeventaid_supplier BIGINT   ENCODE az64
	,vobservations VARCHAR(1000)   ENCODE lzo
	,ipuntodeventaid BIGINT NOT NULL  ENCODE az64
	,fecha_carga TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,PRIMARY KEY (bitransactiondetailid, ipuntodeventaid)
)