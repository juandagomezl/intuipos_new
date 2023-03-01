SELECT  
    --// TRX HEADER
    tTranx.biTransactionID							--// TRX KEY	
    ,tTranx.iPuntoDeVentaID	as PointOfSaleID		--// TRX KEY
    ,tTranxType.tiTransactionTypeID as TransactionTypeID
    ,tTranxType.vName as TransactionTypeName
    ,tDT.fDiscountPercentage as ItemDiscountRate
    ,tTranx.biDocumentID as DocumentID
    ,tTranx.vExternalDocumentID as ExternalDocumentID
    ,tTranx.vObservations
    ,tTranx.dtDueDate
    ,tTranx.dtDate as TransactionInventoryDate
    ,tTranx.tiStatusID as InvDocStatusID
    ,tTrxStatus.vName as InvTrxStatus
    --// TRX DETAILS
    ,tDT.biTransactionDetailID
    ,tDT.iItemID	AS ItemID				--// ITEM FK
    ,tDT.mItemPrice as ItemCost
    ,tDT.dQuantity AS Quantity
    ,tDT.dQuantity * tDT.mItemPrice as TotalItemRowCost
    ,tDT.tiMeasureUnitID AS MeasureUnitID_Detail
    ,tItem.tiMeasureUnitID_Inventory::integer as MeasureUnitID_Inventory
    ,tMeasureUnit_Inventory.vName as MeasureUnitName_Indventory
    ,tItem.tiMeasureUnitID_Recipe as MeasureUnitID_Recipe
    ,tMeasureUnit_Recipe.vName as MeasureUnitName_Recipe	
    ,tDT.tiDetailStatusID	
    ,tDT.iWareHouseID as WareHouseID
    ,tWareHouse.vName as WareHouseName
    ,tDT.dtDate
    ,tDT.iPeriodo AS InventoryPeriod 
    ,tItem.tiMeasureUnitID_Inventory::integer  as tiMeasureUnitID_Purchase
    ,tDT.dQuantity AS dQuantity_Purchase		--// TEMPLATE FOR REQ.COMPRA.
    ,convert(decimal(18,4),1) AS dFactor			--// TEMPLATE FOR REQ.COMPRA.
    ,tDT.biFatherID as biTransactionOrderDetailID
    --// TAX
    ,tDT.iTaxID as TaxID
    ,isnull(tDT.dTaxPercentage,0) as TaxRate
    ,(isnull(tDT.dTaxPercentage,0) / 100) * tDT.mItemPrice as TaxTotal
    --// TAX 2
    ,tDT.iTaxID2 as Tax2ID
    ,isnull(tDT.dTaxPercentage2,0) as Tax2Rate
    ,(isnull(tDT.dTaxPercentage2,0) / 100) * tDT.mItemPrice as Tax2Total
    ,tItem.vName as ItemName
    ,tItem.tiGroupID as GroupID
    ,tGroup.vName as GroupName
    ,tItem.tiSubGroupID as SubGroupID
    ,tSubGroup.vName as SubGroupName
    --// SUPPLIER
    ,tSupplier.iSupplierID AS SupplierID
    ,tSupplier.vNombres as SupplierName
    ,tSupplier.vApellidos as SupplierLastName
    ,tSupplier.vPhoneNumber1 as SupplierPhoneNumber
    ,tSupplier.vAddress as SupplierAddress
    ,tSupplier.vContactName as SupplierContactName
    ,tSupplier.vCedula as SupplierDocumentID
    --// SUPPLIER DETAIL 
    ,tDT.iSupplierID as iSupplierID_OnDetail
    ,tSupplierOnDetail.vNombres as SupplierName_OnDetail
    --Nuevo campo para cierre
    , case when tTranx.tistatusid = 1 and tDT.tiDetailStatusID in (1,4,5,6,7,8,11,12) then tDT.dQuantity * tDT.mItemPrice end as TotalItemRowCost_cierre,
    tTranx.dtlastupdate as t_tranx_dtlastupdate,
    tDT.dtlastupdate as t_trand_dtlastupdate
FROM intuipos_mexico.t_Transaction as tTranx
    inner join intuipos_mexico.t_TransactionDetail as tDT	
        on  tTranx.biTransactionID = tDT.biTransactionID
        and tTranx.iPuntoDeVentaID = tDT.iPuntoDeVentaID 
INNER JOIN intuipos_mexico.T_TrxStatus as tTrxStatus
    on tTranx.tiStatusID = tTrxStatus.tiTrxStatusID
inner join intuipos_mexico.T_TransactionType as tTranxType
    on tTranx.tiTransactionTypeID = tTranxType.tiTransactionTypeID
inner join intuipos_mexico.t_Item as tItem
    on tDT.iItemID = tItem.iItemID
    and tDT.iPuntoDeVentaID_Item = tItem.iPuntoDeVentaID
inner join intuipos_mexico.T_SubGroup as tSubGroup
    on tItem.tiSubGroupID = tSubGroup.tiSubGroupID
    and tItem.iPuntoDeVentaID = tSubGroup.iPuntoDeVentaID
inner join intuipos_mexico.T_Group as tGroup
    on tItem.tiGroupID = tGroup.tiGroupID
    and tItem.iPuntoDeVentaID = tGroup.iPuntoDeVentaID
inner join intuipos_mexico.T_MeasureUnit as tMeasureUnit_Inventory
    on tDT.tiMeasureUnitID = tMeasureUnit_Inventory.tiMeasureUnitID
    and tDT.iPuntoDeVentaID_Item = tMeasureUnit_Inventory.iPuntoDeVentaID
inner join intuipos_mexico.T_WareHouse as tWareHouse
    on tWareHouse.iWareHouseID = tDT.iWareHouseID
left join intuipos_mexico.T_MeasureUnit as tMeasureUnit_Recipe
    on tItem.tiMeasureUnitID_Recipe = tMeasureUnit_Recipe.tiMeasureUnitID
    and tItem.iPuntoDeVentaID = tMeasureUnit_Recipe.iPuntoDeVentaID
left join intuipos_mexico.t_Supplier as tSupplier 
    on tTranx.iSupplierID = tSupplier.iSupplierID 
    and tTranx.iPuntoDeVentaID_Supplier = tSupplier.iPuntoDeVentaID 	
left join intuipos_mexico.t_Supplier as tSupplierOnDetail 
    on tDT.iSupplierID = tSupplierOnDetail.iSupplierID
where tTranx.dtlastupdate >= {t_tranx_dtlastupdate}
    OR tDT.dtlastupdate >= {t_trand_dtlastupdate}