CREATE TABLE intuipos_mexico.t_transaction__stg
(
	bitransactionid BIGINT   ENCODE az64
	,titransactiontypeid BIGINT   ENCODE az64
	,vterminalid VARCHAR(130)   ENCODE lzo
	,mtotalamount NUMERIC(18,4)   ENCODE az64
	,tistatusid BIGINT   ENCODE az64
	,tideliverytypeid BIGINT   ENCODE az64
	,vobservations VARCHAR(390)   ENCODE lzo
	,titableid BIGINT   ENCODE az64
	,biemployeeid BIGINT   ENCODE az64
	,bidocumentid BIGINT   ENCODE az64
	,ipax BIGINT   ENCODE az64
	,biclientid BIGINT   ENCODE az64
	,nombreenpedido VARCHAR(500)   ENCODE lzo
	,iwarehouseid BIGINT   ENCODE az64
	,tireplicationstatusid BIGINT   ENCODE az64
	,ipuntodeventaid BIGINT   ENCODE az64
	,ipuntodeventaid_persona BIGINT   ENCODE az64
	,isupplierid BIGINT   ENCODE az64
	,ipuntodeventaid_supplier BIGINT   ENCODE az64
	,ipuntodeventaid_client BIGINT   ENCODE az64
	,vinvoiceprefix VARCHAR(100)   ENCODE lzo
	,ipuntodeventaid_terminal BIGINT   ENCODE az64
	,ipuntodeventaid_furniture BIGINT   ENCODE az64
	,isubpuntodeventaid BIGINT   ENCODE az64
	,tiplatformid VARCHAR(100)   ENCODE lzo
	,dtduedate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,dtsystemdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,dtdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,vexternaldocumentid VARCHAR(21)   ENCODE lzo
	,vresolucionimpuestosid VARCHAR(21)   ENCODE lzo
	,fecha_carga TIMESTAMP WITHOUT TIME ZONE  DEFAULT now() ENCODE az64
)