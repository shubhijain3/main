--------------------------------------------------------
--  File created - Tuesday-December-20-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package Body XXHTZ_EMEA_PO_INT_IMPORT_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "HTZ_CUSTOM"."XXHTZ_EMEA_PO_INT_IMPORT_PKG" AS
/*****************************************************************
OBJECT NAME: Common EMEA PO Inbound  interface package
DESCRIPTION: Common EMEA PO Inbound  interface package

Version 	Name              	Date           		Description
----------------------------------------------------------------------------
<1.0>		Lakshman Kaigala    26-SEP-2022 	    V1.0- Initial Draft
*****************************************************************/

/*
*********************************************************************
*

*
*********************************************************************
*/

/*
    PROCEDURE xx_update_staging_tbl (p_deliver_to_location_tab    deliver_to_location_tab,
									p_bill_to_location_tab                  bill_to_location_tab,
								    p_Load_Flow_ID        		    IN   NUMBER,
							        p_status              		    OUT  VARCHAR2
								   )
	IS

	PRAGMA AUTONOMOUS_TRANSACTION;

    BEGIN


		IF	p_deliver_to_location_tab.COUNT	>	0
		THEN
		FORALL i IN p_deliver_to_location_tab.FIRST .. p_deliver_to_location_tab.LAST
			UPDATE 	XXHTZ_PO_EMEA_FILE_INBOUND_STG
			   SET Ship_To_site_acct_Num		= 	TRIM (p_deliver_to_location_tab (i).oracle_deliver_to_location_id)
             WHERE 	Billing_Bank				=	p_cust_Deliver_to_location_tab (i).deliver_to_location_code
			   AND 	Flow_ID                  	= 	p_Load_Flow_ID;
		END IF;

        IF	p_bill_to_location_tab.COUNT	>	0
		THEN
		FORALL i IN bill_to_location_tab.FIRST .. bill_to_location_tab.LAST
			UPDATE 	XXHTZ_PO_EMEA_FILE_INBOUND_STG
			   SET BILL_TO_SITE_ACCT_NUM		= 	TRIM (bill_to_location_tab (i).oracle_bill_to_location_id)
             WHERE 	CUSTOMER_ACCOUNT				=	bill_to_location_tab (i).Vendor_num
			   AND 	Flow_ID                  	= 	p_Load_Flow_ID;
		END IF;


		COMMIT;
		p_status := 'SUCCESS';
   EXCEPTION
      WHEN OTHERS
      THEN
         p_status := 'ERROR';
   END ;

  */ 



/*PROCEDURE xx_validate_file_data(p_file_name VARCHAR2, p_flow_id NUMBER)
IS

 g_main_exception EXCEPTION;
 g_error_msg  VARCHAR2(4000);
TYPE t_get_prestage_txn_tbl IS TABLE OF XXHTZ_EMEA_PO_INBOUND_STG_TABLE%ROWTYPE;
l_get_prestage_txn_tbl    t_get_prestage_txn_tbl := t_get_prestage_txn_tbl ();

l_limit         PLS_INTEGER := 10000;

--p_file_id Varchar2(100) := &p_file_id
CURSOR cr_get_prestage_txn IS
SELECT *  FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE a where 1 = 1
and a.OIC_INSTANCE_ID =  p_flow_id
and a.file_name =  p_file_name;



BEGIN

dbms_output.put_line('started...');


  OPEN cr_get_prestage_txn;

    LOOP

	l_get_prestage_txn_tbl.delete;


      FETCH cr_get_prestage_txn BULK COLLECT
       INTO l_get_prestage_txn_tbl LIMIT l_limit;

     FOR i IN 1 .. l_get_prestage_txn_tbl.count LOOP	   

     dbms_output.put_line('Record ID     :' || l_get_prestage_txn_tbl(i).record_id);

	 l_get_prestage_txn_tbl(i).error_message := NULL;
        l_get_prestage_txn_tbl(i).status := 'N';    


        IF l_get_prestage_txn_tbl(i).CURRENCY_CODE IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'CURRENCY_CODE is null',1,4000);
        END IF;

		IF l_get_prestage_txn_tbl(i).VENDOR_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'VENDOR_ID is null',1,4000);
        END IF;

		IF l_get_prestage_txn_tbl(i).VENDOR_NAME IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'VENDOR_NAME is null',1,4000);
        END IF;

		IF l_get_prestage_txn_tbl(i).VENDOR_SITE_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'VENDOR_SITE_ID is null',1,4000);
        END IF;

		IF l_get_prestage_txn_tbl(i).AGENT_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'AGENT_ID is null',1,4000);
        END IF;

		IF l_get_prestage_txn_tbl(i).TERM_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'TERM_ID is null',1,4000);
        END IF;

        l_get_prestage_txn_tbl(i).error_message := ltrim(l_get_prestage_txn_tbl(i)
                                                         .error_message,
                                                         ',');

        IF l_get_prestage_txn_tbl(i).error_message IS NOT NULL THEN
           l_get_prestage_txn_tbl(i).status := 'E';

        END IF;

    END LOOP;

	      --Update records into prestage table
      IF l_get_prestage_txn_tbl.count > 0 THEN
        dbms_output.put_line('Bulk Updating records in pre-stage table');

        FORALL i IN l_get_prestage_txn_tbl.first .. l_get_prestage_txn_tbl.last SAVE   EXCEPTIONS
          UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
		  SET   status            = l_get_prestage_txn_tbl(i).status,
                 error_message     = l_get_prestage_txn_tbl(i).error_message
           WHERE file_id = l_get_prestage_txn_tbl(i).file_id;


        --COMMIT;
        dbms_output.put_line('Update Success');
      END IF;

      EXIT WHEN l_get_prestage_txn_tbl.count < l_limit;
    END LOOP;

    CLOSE cr_get_prestage_txn;

 EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;
END ;*/

 PROCEDURE xx_update_status( p_err_det_type 	IN  x_err_det_type
						   , p_error_status		IN  VARCHAR2
						   , p_success_status	IN  VARCHAR2
						   , p_oic_flow_id		IN  VARCHAR2
						   , p_total_rec_count	OUT NUMBER
						   , p_status_out       OUT VARCHAR2
						   , p_err_msg_out      OUT VARCHAR2 
						   ) IS
 -- g_main_exception EXCEPTION;
  x_err_detail_rec_type 		x_err_det_type;
  --p_total_rec_count number:=0;
 g_error_msg varchar2(4000);
 BEGIN
 x_err_detail_rec_type := p_err_det_type;	

		--Get total record count
		SELECT COUNT(1) 
		  INTO p_total_rec_count 
		  FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE 
		 WHERE oic_instance_id		= p_oic_flow_id;

FOR i IN x_err_detail_rec_type.FIRST..x_err_detail_rec_type.LAST
loop
    IF p_total_rec_count>0 then
    /*For UK and DE */
    UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
    SET
    PO_HEADER_ID=x_err_detail_rec_type(i).po_header_id,
    status=(case when x_err_detail_rec_type(i).interface_status ='ACCEPTED' THEN p_success_status
                 when x_err_detail_rec_type(i).interface_status <> 'ACCEPTED' THEN p_error_status
            END),

            --added on 1 dec
    po_interface_status = (case when x_err_detail_rec_type(i).interface_status ='ACCEPTED' THEN 'CREATED'
                 when x_err_detail_rec_type(i).interface_status <> 'ACCEPTED' THEN 'IMPORT_ERROR'
            END),

    error_type=(case when x_err_detail_rec_type(i).interface_status <> 'ACCEPTED' THEN
                 'POST_IMPORT_VALIDATION_FAILURE' end),
    record_status=x_err_detail_rec_type(i).interface_status,
    error_code=x_err_detail_rec_type(i).error_code,
    error_message=x_err_detail_rec_type(i).error_message
    WHERE
    country_code not in ( 'IT', 'ES')  AND
    po_number=x_err_detail_rec_type(i).po_number and
    oic_instance_id=p_oic_flow_id and
    NVL(status,'A') <>'E';


     UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
    SET
    po_number=x_err_detail_rec_type(i).po_number,
    PO_HEADER_ID=x_err_detail_rec_type(i).po_header_id,
    status=(case when x_err_detail_rec_type(i).interface_status ='ACCEPTED' THEN p_success_status
                 when x_err_detail_rec_type(i).interface_status <>'ACCEPTED' THEN p_error_status
            END),

            --added on 1 dec
    po_interface_status = (case when x_err_detail_rec_type(i).interface_status ='ACCEPTED' THEN 'CREATED'
                 when x_err_detail_rec_type(i).interface_status <> 'ACCEPTED' THEN 'IMPORT_ERROR'
            END),
   error_type=(case when x_err_detail_rec_type(i).interface_status <> 'ACCEPTED' THEN
                 'POST_IMPORT_VALIDATION_FAILURE' end),
    record_status=x_err_detail_rec_type(i).interface_status,
    error_code=x_err_detail_rec_type(i).error_code,
    error_message=x_err_detail_rec_type(i).error_message
    WHERE
    country_code in ( 'IT' , 'ES')  AND
    oic_instance_id=p_oic_flow_id AND
    org_id=x_err_detail_rec_type(i).org_id and
    --agent_id=x_err_detail_rec_type(i).agent_id and 
    vendor_id=x_err_detail_rec_type(i).vendor_id and
    PO_HEADER_DESC=x_err_detail_rec_type(i).po_comments;

    end if;

end loop;
commit;
p_status_out := 'S';

  EXCEPTION
    WHEN OTHERS THEN
    p_status_out 	:= 'E' ;
	p_err_msg_out 	:= SQLERRM;
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
     -- RAISE g_main_exception;
END ;
  ---------------------------------------------------------------------------------------------------------------------------
PROCEDURE XX_PO_INT_ASYNC_CALL_1(p_file_name varchar2, p_flow_id number,p_job_name OUT varchar2)
IS
 --g_main_exception EXCEPTION;
 g_error_msg  VARCHAR2(4000);

v_plSqlBlock varchar2(500);
v_job_int binary_integer;
l_job_name varchar2(100);
l_line_ship_loc varchar2(500):=null;
l_charge_code varchar2(100):=null;
l_purchasing_category varchar2(240):=null;
l_segment1 varchar2(100):=null;
l_segment2 varchar2(100):=null;
l_segment3 varchar2(100):=null;
l_segment4 varchar2(100):=null;

x_out_status VARCHAR2(10);
x_out_msg VARCHAR2(1000);


TYPE t_get_prestage_txn_tbl IS TABLE OF XXHTZ_EMEA_PO_INBOUND_STG_TABLE%ROWTYPE;
l_get_prestage_txn_tbl    t_get_prestage_txn_tbl := t_get_prestage_txn_tbl ();
l_limit         PLS_INTEGER := 10000;


CURSOR cr_get_prestage_txn IS
SELECT *  
  FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE a 
 where 1 = 1
   and a.OIC_INSTANCE_ID =  p_flow_id
   and a.file_name =  p_file_name
   and (a.status='N' or a.status is null)
   AND a.action_type = 'CREATE';

CURSOR cr_po_line_ship_loc(p_loc in varchar2) IS
select DESCRIPTION from xxhtz_fah_fnd_lookup_values where lookup_type='HERTZ_MNT_EMEA_WWD_HR_LOC_MAP'
and enabled_flag='Y' 
--and (end_date_active is null or end_date_active > sysdate)
and lookup_code=p_loc;

CURSOR cr_charge_code(p_expense_desc in varchar2, p_country_code in varchar2)IS
select DESCRIPTION , tag from xxhtz_fah_fnd_lookup_values where lookup_type='HERTZ_MNT_EMEA_EXPENSE_MAPPING'
and enabled_flag='Y' 
--and (end_date_active is null or end_date_active > sysdate)
and attribute1=p_expense_desc
and attribute2=p_country_code;

CURSOR cr_check_po(p_po_num in varchar2) is
  SELECT po_number  
    FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE a where 1 = 1
     and a.OIC_INSTANCE_ID =  p_flow_id
     and a.file_name =  p_file_name
     and po_number=p_po_num
     and po_number is not null
     group by po_number,OIC_INSTANCE_ID
     having count( po_number)>1;

Cursor cr_update_dup_po(p_po_num in varchar2, P_ID IN NUMBER) 
    is
   select count(*)cnt 
     from XXHTZ_EMEA_PO_INBOUND_STG_TABLE
    where OIC_INSTANCE_ID=p_flow_id
      and file_name =p_file_name
      and po_number=p_po_num
      and NVL(error_code,'A') <>'DUP_PO'
      and ID <> P_ID
      and ID < P_ID;

 c_po_line_ship_loc cr_po_line_ship_loc%ROWTYPE;
 c_charge_code cr_charge_code%ROWTYPE;
 c_check_po cr_check_po%rowtype;
 c_update_dup_po cr_update_dup_po%ROWTYPE;

BEGIN 

   --Calling Update_PO procedure to mark PO records for Update/Cancel 
     update_po (p_oic_instance_id => p_flow_id
              , x_status_out =>  x_out_status
              ,  x_err_msg =>  x_out_msg
              );
      IF x_out_msg IS NOT NULL 
      THEN       
        p_job_name := ' Update_PO error: ' || x_out_msg;

      END IF;

  OPEN cr_get_prestage_txn;

    LOOP

	  l_get_prestage_txn_tbl.delete;

    FETCH cr_get_prestage_txn BULK COLLECT INTO l_get_prestage_txn_tbl LIMIT l_limit;

    FOR i IN 1 .. l_get_prestage_txn_tbl.count LOOP	   

     dbms_output.put_line('Record ID     :' || l_get_prestage_txn_tbl(i).record_id);

	    l_get_prestage_txn_tbl(i).error_message := NULL;
        l_get_prestage_txn_tbl(i).status := 'N';    

     --    l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ',' || 'CURRENCY_CODE is null',1,4000);

		IF l_get_prestage_txn_tbl(i).VENDOR_ID IS NULL or l_get_prestage_txn_tbl(i).VENDOR_SITE_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Vendor',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

		IF l_get_prestage_txn_tbl(i).VENDOR_NAME IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Vendor',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

	/*	IF l_get_prestage_txn_tbl(i).VENDOR_SITE_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Vendor',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;*/

		IF l_get_prestage_txn_tbl(i).AGENT_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Buyer Details',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

		IF l_get_prestage_txn_tbl(i).PAYMENT_TERM IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Payment Terms',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

        --added on 1 dec
        IF l_get_prestage_txn_tbl(i).SHIP_TO_LOCATION_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Ship to Location',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

        --added on 1 dec
        IF l_get_prestage_txn_tbl(i).BILL_TO_LOCATION_ID IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Bill to Location',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

      IF l_get_prestage_txn_tbl(i).po_amount =0 THEN
         l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'PO amount is zero can not create PO',1,4000);
         l_get_prestage_txn_tbl(i).po_interface_status := 'REJECTED'; --added on 1 dec
      END IF;


         IF l_get_prestage_txn_tbl(i).po_number is not null then
        open cr_check_po (l_get_prestage_txn_tbl(i).po_number);
        fetch cr_check_po into c_check_po;
        if cr_check_po%found then
        Open cr_update_dup_po(l_get_prestage_txn_tbl(i).po_number, l_get_prestage_txn_tbl(i).ID);
        FETCH cr_update_dup_po INTO c_update_dup_po;
        IF c_update_dup_po.cnt>0 THEN  
        l_get_prestage_txn_tbl(i).error_code:='DUP_PO';
        l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Same PO number exists multiple times in same PO file',1,4000);
        l_get_prestage_txn_tbl(i).po_interface_status := 'DUPLICATE'; --added on 1 dec
        end if;
        CLOSE cr_update_dup_po;
        end if;
        close cr_check_po;
        c_check_po:=null;
        c_update_dup_po:=null;
        end if;

        IF l_get_prestage_txn_tbl(i).country_code not in ('IT','ES') and l_get_prestage_txn_tbl(i).PO_NUMBER IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'PO Number is null',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

         IF l_get_prestage_txn_tbl(i).country_code  in ('ES') and l_get_prestage_txn_tbl(i).CHARGE_ACCOUNT IS NULL THEN
           l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Charge Account is null for Spain',1,4000);
           l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        END IF;

        OPEN cr_po_line_ship_loc(l_get_prestage_txn_tbl(i).DELIVER_TO_LOC_CODE);
        FETCH cr_po_line_ship_loc INTO c_po_line_ship_loc;

        IF cr_po_line_ship_loc%NOTFOUND 
        THEN
          l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error deriving Deliver to Location',1,4000);
          l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        ELSE 
          l_line_ship_loc:=c_po_line_ship_loc.DESCRIPTION;
        END IF;

        CLOSE cr_po_line_ship_loc;

        c_po_line_ship_loc:=null;


        OPEN cr_charge_code(l_get_prestage_txn_tbl(i).expense_desc, l_get_prestage_txn_tbl(i).country_code);
        FETCH cr_charge_code INTO c_charge_code;
        IF cr_charge_code%NOTFOUND 
        THEN
          l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error Deriving Charge Account from Lookup',1,4000);
          l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD'; --added on 1 dec
        ELSE 
          l_charge_code:=c_charge_code.DESCRIPTION;
          l_purchasing_category:=c_charge_code.tag;
        END IF;

        CLOSE cr_charge_code;

         IF l_get_prestage_txn_tbl(i).country_code in ('IT', 'DE')  
         THEN 
          l_charge_code:=l_charge_code;

        ELSIF l_get_prestage_txn_tbl(i).country_code in ('UK', 'GB')  
        THEN
       BEGIN  --added on 1 dec
          IF l_get_prestage_txn_tbl(i).charge_to_loc_code <>l_get_prestage_txn_tbl(i).deliver_to_loc_code
          then

            /*l_segment1:=XXHTZ_fah_coa_mapping_pkg.get_legal_entity_segment
                                                (p_leg_company => substr(l_charge_code,1,4),
												 p_errmsg=> g_error_msg);*/
			 l_segment1:=XXHTZ_fah_coa_mapping_pkg.get_11i_company_segment
                                                (p_glb_le => substr(l_charge_code,1,4),
												 p_errmsg=> g_error_msg);

            IF l_segment1 IS NULL 
            THEN
              l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error while fetching company code '||g_error_msg,1,4000);
            END IF;

             l_segment2:=XXHTZ_fah_coa_mapping_pkg.get_brand_segment
                                                    (p_leg_company => l_segment1,
                                                     p_leg_cost_center =>l_get_prestage_txn_tbl(i).charge_to_loc_code,
												 p_errmsg=> g_error_msg);
            IF l_segment2 IS NULL 
            THEN
              l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error while fetching Brand '||g_error_msg,1,4000);
            END IF;

            l_segment3:=XXHTZ_fah_coa_mapping_pkg.get_location_segment
                                                  (p_leg_company => l_segment1,
                                                   p_leg_cost_center =>l_get_prestage_txn_tbl(i).charge_to_loc_code,
                                                    p_errmsg=> g_error_msg);

           IF l_segment3 IS NULL 
           THEN
              l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error while fetching Location '||g_error_msg,1,4000);
           END IF;

           l_segment4:=XXHTZ_fah_coa_mapping_pkg.get_department_segment
                                                  (p_leg_company => l_segment1,
                                                   p_leg_cost_center =>l_get_prestage_txn_tbl(i).charge_to_loc_code,
                                                  p_errmsg=> g_error_msg);
           IF l_segment4 IS NULL 
           THEN
              l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Error while fetching Department '||g_error_msg,1,4000);
           END IF;

           l_charge_code:= substr (l_charge_code,1,4)||'-'|| --l_segment1 ||'-'||
                           l_segment2 ||'-'||
                           l_segment3 ||'-'||
                           l_segment4 ||'-'||
                           substr (l_charge_code,22,4)||'-'||
                           substr(l_charge_code,27,5)||'-'||
                          substr(l_charge_code,33,4)||'-'||
                           substr(l_charge_code,38,2)||'-'||
                           substr(l_charge_code,41,7)||'-'||
                            substr(l_charge_code,49,7);
            l_purchasing_category:=l_purchasing_category;
          else
            l_charge_code:=l_charge_code;
            l_purchasing_category:=l_purchasing_category;
          end if;

         EXCEPTION
                     WHEN OTHERS
                     THEN
                       l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Unable to Derive correct Code Combination for UK',1,4000);
                        l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD';
                  END;
          c_charge_code:=null;

      /*  ELSIF l_get_prestage_txn_tbl(i).country_code in ('UK','GB') THEN
            IF l_get_prestage_txn_tbl(i).CHARGE_TO_LOC_CODE <>l_get_prestage_txn_tbl(i).DELIVER_TO_LOC_CODE THEN
            ELSE
            END IF;*/
        END IF;

        l_get_prestage_txn_tbl(i).error_message := ltrim(l_get_prestage_txn_tbl(i)
                                                         .error_message,
                                                         ',');

        IF l_get_prestage_txn_tbl(i).error_message IS NOT NULL 
        THEN
           l_get_prestage_txn_tbl(i).status := 'E';
            p_job_name:= p_job_name || ';'|| 'E';

          UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE 
          SET status='E',
            error_code=l_get_prestage_txn_tbl(i).error_code,
            ERROR_MESSAGE=l_get_prestage_txn_tbl(i).error_message,
            po_interface_status=l_get_prestage_txn_tbl(i).po_interface_status,
            error_type='PRE_IMPORT_VALIDATION_FAILURE'
            WHERE OIC_INSTANCE_ID =  p_flow_id
              and file_name =  p_file_name
              and ID=l_get_prestage_txn_tbl(i).ID;
        COMMIT;

        ELSE
          p_job_name:= p_job_name || ';' || 'V';
          l_get_prestage_txn_tbl(i).status := 'V';
          UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE 
            SET     status='V',
                ERROR_MESSAGE='Validated',
                 ship_to_location=l_line_ship_loc,
                 charge_account=l_charge_code,
                 purchasing_category=l_purchasing_category
           WHERE OIC_INSTANCE_ID =  p_flow_id
             and file_name =  p_file_name
             and ID=l_get_prestage_txn_tbl(i).ID;

           COMMIT;
        END IF;

    l_line_ship_loc:=null;
    l_charge_code:=null;
    l_purchasing_category:=null;
    END LOOP;

	      --Update records into prestage table
    /*  IF l_get_prestage_txn_tbl.count > 0 THEN
        dbms_output.put_line('Bulk Updating records in pre-stage table');

        FORALL i IN l_get_prestage_txn_tbl.first .. l_get_prestage_txn_tbl.last SAVE   EXCEPTIONS
          UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
		  SET   status            = l_get_prestage_txn_tbl(i).status,
                 error_message     = l_get_prestage_txn_tbl(i).error_message
           WHERE file_id = l_get_prestage_txn_tbl(i).file_id;


        --COMMIT;
        dbms_output.put_line('Update Success');
      END IF;*/

      EXIT WHEN l_get_prestage_txn_tbl.count < l_limit;
    END LOOP;

    CLOSE cr_get_prestage_txn;

      p_job_name:= p_job_name ||';'||'V';

      EXCEPTION
      WHEN OTHERS THEN
      p_job_name:='Unexpected Error  : ' || SQLCODE || '-' || SQLERRM;
        g_error_msg := 'Unexpected Error  : ' || SQLCODE || '-' || SQLERRM;
      --RAISE g_main_exception;
END;



PROCEDURE xx_INSERT_FBDI_DATA(p_file_name VARCHAR2, p_flow_id NUMBER,p_status OUT VARCHAR2) 
IS 
 g_main_exception EXCEPTION;
 g_error_msg  VARCHAR2(4000);
TYPE t_get_prestage_txn_tbl IS TABLE OF XXHTZ_EMEA_PO_INBOUND_STG_TABLE%ROWTYPE;
l_get_prestage_txn_tbl    t_get_prestage_txn_tbl := t_get_prestage_txn_tbl ();

    TYPE t_Headers_load_tbl IS TABLE OF XXHTZ_PO_EMEA_FBDI_HEADERS_STG_TBL%ROWTYPE;

    l_headers_load_tbl       t_Headers_load_tbl := t_Headers_load_tbl();

    TYPE t_lines_load_tbl IS TABLE OF XXHTZ_PO_EMEA_FBDI_LINES_STG_TBL%ROWTYPE;

    l_lines_load_tbl       t_lines_load_tbl := t_lines_load_tbl();

    TYPE t_line_loc_load_tbl IS TABLE OF XXHTZ_PO_EMEA_FBDI_LINE_LOCATION_STG_TBL%ROWTYPE;

    l_line_loc_load_tbl       t_line_loc_load_tbl := t_line_loc_load_tbl();

    TYPE t_distributions_load_tbl IS TABLE OF XXHTZ_PO_EMEA_FBDI_DISTIBUTIONS_STG_TBL%ROWTYPE;

    l_distributions_load_tbl       t_distributions_load_tbl := t_distributions_load_tbl();

    x_hdr_idx                     PLS_INTEGER DEFAULT 0;
    x_line_idx                     PLS_INTEGER DEFAULT 0;
    x_line_loc_idx                  PLS_INTEGER DEFAULT 0;
    x_distribution_idx             PLS_INTEGER DEFAULT 0;


l_limit         PLS_INTEGER := 10000;

v_interface_source_code   VARCHAR2 (20);
v_po_hdr_desc             VARCHAR2 (200);
v_po_line_desc            VARCHAR2(240);
v_charge_account1         VARCHAR2(240);
v_charge_account2         VARCHAR2(240);
v_charge_account3         VARCHAR2(240);
v_charge_account4         VARCHAR2(240);
v_charge_account5         VARCHAR2(240);
v_charge_account6         VARCHAR2(240);
v_charge_account7         VARCHAR2(240);
v_charge_account8         VARCHAR2(240);
v_charge_account9         VARCHAR2(240);
v_charge_account10         VARCHAR2(240);


--p_file_id Varchar2(100) := &p_file_id
CURSOR cr_get_prestage_txn IS
SELECT *  FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE a where 1 = 1
and a.OIC_INSTANCE_ID =  p_flow_id
and a.file_name =  p_file_name
and status='V'
AND action_type = 'CREATE';

CURSOR CR_LINE_ATT_LIC_DATA(p_license in varchar2)  IS
select license,
vin,
unit,
model,
veh_desc,
model_yr,
country,
area_num,
type_code,
mileage,
cv_indicator,
safety_recall from xxhtz_po_car_info_all
where  license=p_license;

CURSOR CR_LINE_ATT_DATA(p_unit in varchar2,p_license in varchar2)  IS
select license,
vin,
unit,
model,
veh_desc,
model_yr,
country,
area_num,
type_code,
mileage,
cv_indicator,
safety_recall from xxhtz_po_car_info_all
where unit=p_unit 
and license=p_license;

C_LINE_ATT_LIC_DATA CR_LINE_ATT_LIC_DATA%ROWTYPE;
C_LINE_ATT_DATA CR_LINE_ATT_DATA%rowtype;

BEGIN

dbms_output.put_line('started...');


  OPEN cr_get_prestage_txn;

    LOOP

    l_get_prestage_txn_tbl.delete;
    l_headers_load_tbl.delete;
    l_lines_load_tbl.delete;
    l_line_loc_load_tbl.delete;


      FETCH cr_get_prestage_txn BULK COLLECT
       INTO l_get_prestage_txn_tbl LIMIT l_limit;

     FOR i IN 1 .. l_get_prestage_txn_tbl.count LOOP       

                         x_hdr_idx := x_hdr_idx + 1;
                         x_line_idx := x_line_idx + 1;
                         x_line_loc_idx:=x_line_loc_idx+1;
                         x_distribution_idx := x_distribution_idx + 1;



            l_headers_load_tbl.extend;
            l_lines_load_tbl.extend;
            l_line_loc_load_tbl.extend;
            l_distributions_load_tbl.extend;



        IF l_get_prestage_txn_tbl(i).COUNTRY_CODE = 'IT'
            THEN
            v_interface_source_code := 'WEBCAR';
            ELSE
            v_interface_source_code := 'MNT';
         END IF;

        v_po_hdr_desc := l_get_prestage_txn_tbl(i).po_header_desc;

         IF l_get_prestage_txn_tbl(i).COUNTRY_CODE ='IT'
        THEN
           v_po_line_desc := SUBSTRB(v_po_hdr_desc || ' - ' ||l_get_prestage_txn_tbl(i).expense_desc,1,240);
        ELSE

           v_po_line_desc := l_get_prestage_txn_tbl(i).expense_desc; 
        END IF;  

		--	IF l_get_prestage_txn_tbl(i).COUNTRY_CODE = 'ES'
       --     THEN
             v_charge_account1 := substr(l_get_prestage_txn_tbl(i).charge_account,1,4);
			 v_charge_account2 := substr(l_get_prestage_txn_tbl(i).charge_account,6,2);
			 v_charge_account3 := substr(l_get_prestage_txn_tbl(i).charge_account,9,7);
			 v_charge_account4 := substr(l_get_prestage_txn_tbl(i).charge_account,17,4);
			 v_charge_account5 := substr(l_get_prestage_txn_tbl(i).charge_account,22,4);
			 v_charge_account6 := substr(l_get_prestage_txn_tbl(i).charge_account,27,5);
			 v_charge_account7 := substr(l_get_prestage_txn_tbl(i).charge_account,33,4);
			 v_charge_account8 := substr(l_get_prestage_txn_tbl(i).charge_account,38,2);
			 v_charge_account9 := substr(l_get_prestage_txn_tbl(i).charge_account,41,7);
			 v_charge_account10 := substr(l_get_prestage_txn_tbl(i).charge_account,49,7);

       --    END IF;

                l_headers_load_tbl(x_hdr_idx).INTERFACE_HEADER_ID := x_hdr_idx;                
                l_headers_load_tbl(x_hdr_idx).FILE_ID := 1;
                l_headers_load_tbl(x_hdr_idx).flow_id := l_get_prestage_txn_tbl(i).OIC_INSTANCE_ID;
                l_headers_load_tbl(x_hdr_idx).ACTION  := 'ORIGINAL';
                l_headers_load_tbl(x_hdr_idx).batch_id:=l_get_prestage_txn_tbl(i).OIC_INSTANCE_ID;
                l_headers_load_tbl(x_hdr_idx).approval_action:='SUBMIT';
                l_headers_load_tbl(x_hdr_idx).import_source:='MNTUPLOAD';
                l_headers_load_tbl(x_hdr_idx).document_type_code  := 'STANDARD';
                l_headers_load_tbl(x_hdr_idx).vendor_name := l_get_prestage_txn_tbl(i).vendor_name;
                l_headers_load_tbl(x_hdr_idx).vendor_num:=l_get_prestage_txn_tbl(i).vendor_number;
                l_headers_load_tbl(x_hdr_idx).PRC_BU_NAME:= l_get_prestage_txn_tbl(i).OU_NAME;
                l_headers_load_tbl(x_hdr_idx).REQ_BU_NAME:= l_get_prestage_txn_tbl(i).OU_NAME;
                l_headers_load_tbl(x_hdr_idx).SOLDTO_LE_NAME:= l_get_prestage_txn_tbl(i).LEGAL_ENTITY_NAME;
                l_headers_load_tbl(x_hdr_idx).BILLTO_BU_NAME:= l_get_prestage_txn_tbl(i).OU_NAME;
                l_headers_load_tbl(x_hdr_idx).CURRENCY_CODE:= l_get_prestage_txn_tbl(i).currency_code;
                l_headers_load_tbl(x_hdr_idx).vendor_site_code := l_get_prestage_txn_tbl(i).site_name;
                l_headers_load_tbl(x_hdr_idx).ship_to_location := l_get_prestage_txn_tbl(i).ship_to_location_id;
                l_headers_load_tbl(x_hdr_idx).bill_to_location := l_get_prestage_txn_tbl(i).bill_to_location_id;
                l_headers_load_tbl(x_hdr_idx).payment_terms :=  l_get_prestage_txn_tbl(i).PAYMENT_TERM;
                l_headers_load_tbl(x_hdr_idx).initiating_party:='BUYER';
                l_headers_load_tbl(x_hdr_idx).REQUIRED_ACKNOWLEDGMENT_FLAG:='N';
                l_headers_load_tbl(x_hdr_idx).interface_source_code := NULL;
                l_headers_load_tbl(x_hdr_idx).document_num := l_get_prestage_txn_tbl(i).PO_NUMBER;
                l_headers_load_tbl(x_hdr_idx).agent_name := l_get_prestage_txn_tbl(i).agent_name; 
                l_headers_load_tbl(x_hdr_idx).comments:=v_po_hdr_desc;
                --l_headers_load_tbl(x_hdr_idx).org_id := l_get_prestage_txn_tbl(i).org_id;

       -- FORALL i IN l_headers_load_tbl.first .. l_headers_load_tbl.last SAVE                EXCEPTIONS

       BEGIN


          INSERT INTO XXHTZ_PO_EMEA_FBDI_HEADERS_STG_TBL (INTERFACE_HEADER_ID,
                                                          FILE_ID,
                                                          FLOW_ID,
                                                          ACTION,
                                                          document_type_code,
                                                          vendor_name,
                                                          vendor_site_code,
                                                          ship_to_location,
                                                          interface_source_code,
                                                          document_num,
                                                          agent_name,
                                                          status,
                                                          batch_id,
                                                          approval_action,
                                                          PRC_BU_NAME,
                                                          REQ_BU_NAME,
                                                          BILLTO_BU_NAME,
                                                          CURRENCY_CODE,
                                                          bill_to_location,
                                                          payment_terms,
                                                          initiating_party,
                                                          REQUIRED_ACKNOWLEDGMENT_FLAG  ,
                                                          import_source,
                                                          SOLDTO_LE_NAME,
                                                          comments
                                                           )
                                                    VALUES ( l_headers_load_tbl(x_hdr_idx).INTERFACE_HEADER_ID,
                                                             l_headers_load_tbl(x_hdr_idx).FILE_ID,
                                                             l_headers_load_tbl(x_hdr_idx).flow_id,
                                                             l_headers_load_tbl(x_hdr_idx).ACTION,
                                                             l_headers_load_tbl(x_hdr_idx).document_type_code,
                                                             l_headers_load_tbl(x_hdr_idx).vendor_name,
                                                             l_headers_load_tbl(x_hdr_idx).vendor_site_code,
                                                             l_headers_load_tbl(x_hdr_idx).ship_to_location,                                                             
                                                             v_interface_source_code,
                                                             l_headers_load_tbl(x_hdr_idx).document_num,
                                                             l_headers_load_tbl(x_hdr_idx).agent_name,
                                                             'V',
                                                              l_headers_load_tbl(x_hdr_idx).batch_id,
                                                              l_headers_load_tbl(x_hdr_idx).approval_action,
                                                              l_headers_load_tbl(x_hdr_idx).PRC_BU_NAME,
                                                              l_headers_load_tbl(x_hdr_idx).REQ_BU_NAME,
                                                              l_headers_load_tbl(x_hdr_idx).BILLTO_BU_NAME,
                                                              l_headers_load_tbl(x_hdr_idx).CURRENCY_CODE,

                                                              l_headers_load_tbl(x_hdr_idx).bill_to_location,
                                                              l_headers_load_tbl(x_hdr_idx).payment_terms,
                                                              l_headers_load_tbl(x_hdr_idx).initiating_party,
                                                              l_headers_load_tbl(x_hdr_idx).REQUIRED_ACKNOWLEDGMENT_FLAG  ,
                                                               l_headers_load_tbl(x_hdr_idx).import_source,
                                                               l_headers_load_tbl(x_hdr_idx).SOLDTO_LE_NAME,
                                                               l_headers_load_tbl(x_hdr_idx).comments
                                                             );
		EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;
			END;


            BEGIN                                                

                    l_lines_load_tbl(x_line_idx).INTERFACE_LINE_ID := x_line_idx;
                    l_lines_load_tbl(x_line_idx).INTERFACE_HEADER_ID := x_hdr_idx;
                    l_lines_load_tbl(x_line_idx).FILE_ID := 1;                  
                    l_lines_load_tbl(x_line_idx).flow_id := l_get_prestage_txn_tbl(i).OIC_INSTANCE_ID;                  
                    l_lines_load_tbl(x_line_idx).line_num := 1;
                    l_lines_load_tbl(x_line_idx).Action :='ADD';
                    l_lines_load_tbl(x_line_idx).item := NULL;
                    l_lines_load_tbl(x_line_idx).Line_type := 'Expense';--l_get_prestage_txn_tbl(i).line_type;            ----need to send from table
                    l_lines_load_tbl(x_line_idx).category_name :=l_get_prestage_txn_tbl(i).purchasing_category;
                    l_lines_load_tbl(x_line_idx).item_description := v_po_line_desc;                ---need to derive
                    l_lines_load_tbl(x_line_idx).AMOUNT :=  l_get_prestage_txn_tbl(i).po_amount;
                    l_lines_load_tbl(x_line_idx).SHIPPING_UOM_QUANTITY:=l_get_prestage_txn_tbl(i).po_amount;
                    l_lines_load_tbl(x_line_idx).SHIPPING_UNIT_OF_MEASURE:='Each';
                    l_lines_load_tbl(x_line_idx).unit_price :=  1   ; 

--insert lines



               INSERT INTO XXHTZ_PO_EMEA_FBDI_LINES_STG_TBL (FILE_ID,
                                                          FLOW_ID,
                                                          INTERFACE_LINE_ID,
                                                          INTERFACE_HEADER_ID,
                                                          line_num,
                                                          item,
                                                          Line_type,
                                                          category_name,
                                                          item_description,
                                                          AMOUNT,
                                                          unit_price,
                                                          status,
                                                          action,
                                                          SHIPPING_UOM_QUANTITY,
                                                          SHIPPING_UNIT_OF_MEASURE
                                                           )
                                                    VALUES ( 
                                                             l_lines_load_tbl(x_line_idx).FILE_ID,                                                           
                                                             l_lines_load_tbl(x_line_idx).flow_id,
                                                             l_lines_load_tbl(x_line_idx).INTERFACE_LINE_ID,
                                                             l_lines_load_tbl(x_line_idx).INTERFACE_HEADER_ID, 
                                                             l_lines_load_tbl(x_line_idx).line_num,
                                                             l_lines_load_tbl(x_line_idx).item,
                                                             l_lines_load_tbl(x_line_idx).Line_type,
                                                             l_lines_load_tbl(x_line_idx).category_name,
                                                             l_lines_load_tbl(x_line_idx).item_description,
                                                             l_lines_load_tbl(x_line_idx).AMOUNT,
                                                             l_lines_load_tbl(x_line_idx).unit_price,
                                                             'V',
                                                              l_lines_load_tbl(x_line_idx).action,
                                                              l_lines_load_tbl(x_line_idx).SHIPPING_UOM_QUANTITY,
                                                              l_lines_load_tbl(x_line_idx).SHIPPING_UNIT_OF_MEASURE
                                                             );


             EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;                                                 



        END;

-- insert line locations
         BEGIN
          l_line_loc_load_tbl(x_line_loc_idx).FILE_ID := 1;                  
         l_line_loc_load_tbl(x_line_loc_idx).flow_id := l_get_prestage_txn_tbl(i).OIC_INSTANCE_ID;     
         l_line_loc_load_tbl(x_line_loc_idx).INTERFACE_LINE_ID := x_line_idx;
         l_line_loc_load_tbl(x_line_loc_idx).INTERFACE_LINE_LOCATION_ID := x_line_loc_idx;
         l_line_loc_load_tbl(x_line_loc_idx).schedule_num:=1;
         l_line_loc_load_tbl(x_line_loc_idx).ship_to_location:=l_get_prestage_txn_tbl(i).ship_to_location;
         l_line_loc_load_tbl(x_line_loc_idx).amount:=l_get_prestage_txn_tbl(i).po_amount;
         l_line_loc_load_tbl(x_line_loc_idx).quantity:=l_get_prestage_txn_tbl(i).po_amount;
         l_line_loc_load_tbl(x_line_loc_idx).need_by_date:=TO_CHAR(TO_DATE(l_get_prestage_txn_tbl(i).need_by_date, 'DD/MM/YYYY'),'YYYY/MM/DD') ;
         l_line_loc_load_tbl(x_line_loc_idx).destination_type_code:='EXPENSE';
    --     l_line_loc_load_tbl(x_line_loc_idx).requested_delivery_date:=null;


               INSERT INTO XXHTZ_PO_EMEA_FBDI_LINE_LOCATION_STG_TBL (FILE_ID,
                                                          FLOW_ID,
                                                          INTERFACE_LINE_ID,
                                                          INTERFACE_LINE_LOCATION_ID,
                                                          schedule_num,
                                                          ship_to_location,
                                                          amount,
                                                          quantity,
                                                          need_by_date,
                                                          destination_type_code,
                                                      --    requested_delivery_date,
                                                          status) values
                                                          ( l_line_loc_load_tbl(x_line_loc_idx).FILE_ID,
                                                             l_line_loc_load_tbl(x_line_loc_idx).flow_id,
                                                            l_line_loc_load_tbl(x_line_loc_idx).INTERFACE_LINE_ID,
                                                            l_line_loc_load_tbl(x_line_loc_idx).INTERFACE_LINE_location_ID,
                                                            l_line_loc_load_tbl(x_line_loc_idx).schedule_num,
                                                             l_line_loc_load_tbl(x_line_loc_idx).ship_to_location,
                                                             l_line_loc_load_tbl(x_line_loc_idx).amount,
                                                             l_line_loc_load_tbl(x_line_loc_idx).quantity,
                                                             l_line_loc_load_tbl(x_line_loc_idx).need_by_date,
                                                             l_line_loc_load_tbl(x_line_loc_idx).destination_type_code,
                                                       --      l_line_loc_load_tbl(x_line_loc_idx).requested_delivery_date,
                                                             'V');



		EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;
			END;
        -- insert distrubutions

        BEGIN      

                    OPEN CR_LINE_ATT_LIC_DATA( l_get_prestage_txn_tbl(i).LICENSE_PLATE);
                    FETCH CR_LINE_ATT_LIC_DATA INTO C_LINE_ATT_LIC_DATA;
                    --added on 1 dec
                    IF CR_LINE_ATT_LIC_DATA%NOTFOUND THEN 
                    l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Failed to validate Car Details',1,4000);
                    l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD';
                    end if;
                    IF CR_LINE_ATT_LIC_DATA%ROWCOUNT>1 THEN
                    OPEN CR_LINE_ATT_DATA ( l_get_prestage_txn_tbl(i).UNIT_NUMBER, l_get_prestage_txn_tbl(i).LICENSE_PLATE);
                    FETCH CR_LINE_ATT_DATA INTO C_LINE_ATT_DATA;
                    --added on 1 dec
                    IF CR_LINE_ATT_DATA%NOTFOUND THEN 
                    l_get_prestage_txn_tbl(i).error_message := substr(l_get_prestage_txn_tbl(i).error_message || ';' || 'Failed to validate Car Details',1,4000);
                    l_get_prestage_txn_tbl(i).po_interface_status := 'INVALID_RECORD';
                    end if;

                    CLOSE CR_LINE_ATT_DATA;
                      l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE1 := C_LINE_ATT_DATA.license;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE2 := C_LINE_ATT_DATA.vin;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE3 := C_LINE_ATT_DATA.unit;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE4 := C_LINE_ATT_DATA.model;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE5 := C_LINE_ATT_DATA.veh_desc;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE6 := C_LINE_ATT_DATA.model_yr;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE7 := C_LINE_ATT_DATA.country;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE8 := C_LINE_ATT_DATA.area_num;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE9 := C_LINE_ATT_DATA.type_code;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE10 :=C_LINE_ATT_DATA.mileage;
                   -- l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE11 :=l_get_prestage_txn_tbl(i).cv_indicator;    
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE12 :=C_LINE_ATT_DATA.cv_indicator;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE13 :=C_LINE_ATT_DATA.safety_recall;

                    ELSE
                      l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE1 := C_LINE_ATT_LIC_DATA.license;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE2 := C_LINE_ATT_LIC_DATA.vin;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE3 := C_LINE_ATT_LIC_DATA.unit;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE4 := C_LINE_ATT_LIC_DATA.model;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE5 := C_LINE_ATT_LIC_DATA.veh_desc;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE6 := C_LINE_ATT_LIC_DATA.model_yr;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE7 := C_LINE_ATT_LIC_DATA.country;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE8 := C_LINE_ATT_LIC_DATA.area_num;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE9 := C_LINE_ATT_LIC_DATA.type_code;
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE10 :=C_LINE_ATT_LIC_DATA.mileage;
                   -- l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE11 :=l_get_prestage_txn_tbl(i).cv_indicator;    
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE12 :=C_LINE_ATT_LIC_DATA.cv_indicator;  
                    l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE13 :=C_LINE_ATT_LIC_DATA.safety_recall;

                    END IF;
                    CLOSE CR_LINE_ATT_LIC_DATA;



                    l_distributions_load_tbl(x_distribution_idx).INTERFACE_DISTRIBUTION_ID := x_distribution_idx;
                    l_distributions_load_tbl(x_distribution_idx).INTERFACE_LINE_LOCATION_ID:=x_line_loc_idx;
                    l_distributions_load_tbl(x_distribution_idx).FILE_ID := 1;                  
                    l_distributions_load_tbl(x_distribution_idx).flow_id := l_get_prestage_txn_tbl(i).OIC_INSTANCE_ID;
                    l_distributions_load_tbl(x_distribution_idx).distribution_num := 1 ;
                     l_distributions_load_tbl(x_distribution_idx).deliver_to_location:=l_get_prestage_txn_tbl(i).ship_to_location;
                      l_distributions_load_tbl(x_distribution_idx).requester:=l_get_prestage_txn_tbl(i).agent_name;
                       l_distributions_load_tbl(x_distribution_idx).AMOUNT_ORDERED:=l_get_prestage_txn_tbl(i).po_amount;
                        l_distributions_load_tbl(x_distribution_idx).SHIPPING_UOM_QUANTITY:=l_get_prestage_txn_tbl(i).po_amount;

                l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT1 := v_charge_account1; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT2 := v_charge_account2; 
					l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT3 := v_charge_account3; 
					l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT4 := v_charge_account4; 
					l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT5 := v_charge_account5; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT6 := v_charge_account6; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT7 := v_charge_account7; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT8 := v_charge_account8; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT9 := v_charge_account9; 
                    l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT10 := v_charge_account10; 					





               INSERT INTO XXHTZ_PO_EMEA_FBDI_DISTIBUTIONS_STG_TBL (FILE_ID,
                                                          FLOW_ID,
                                                          INTERFACE_DISTRIBUTION_ID,
                                                          INTERFACE_LINE_LOCATION_ID,
                                                          distribution_num,
                                                          deliver_to_location,
                                                          requester,
                                                          AMOUNT_ORDERED,
                                                          SHIPPING_UOM_QUANTITY,
                                                          ATTRIBUTE1,  
                                                          ATTRIBUTE2,
                                                          ATTRIBUTE3,
                                                          ATTRIBUTE4,
                                                          ATTRIBUTE5,
                                                          ATTRIBUTE6,
                                                          ATTRIBUTE7,
                                                          ATTRIBUTE8,
                                                          ATTRIBUTE9,
                                                          ATTRIBUTE10,
                                                          ATTRIBUTE12,
                                                          ATTRIBUTE13,
														  CHARGE_ACCOUNT_SEGMENT1,
														  CHARGE_ACCOUNT_SEGMENT2,
														  CHARGE_ACCOUNT_SEGMENT3,
														  CHARGE_ACCOUNT_SEGMENT4,
														  CHARGE_ACCOUNT_SEGMENT5,
														  CHARGE_ACCOUNT_SEGMENT6,
														  CHARGE_ACCOUNT_SEGMENT7,
														  CHARGE_ACCOUNT_SEGMENT8,
														  CHARGE_ACCOUNT_SEGMENT9,
														  CHARGE_ACCOUNT_SEGMENT10,
                                                          status
                                                           )
                                                    VALUES ( 
                                                             l_distributions_load_tbl(x_distribution_idx).FILE_ID,                                                           
                                                             l_distributions_load_tbl(x_distribution_idx).flow_id,
                                                             l_distributions_load_tbl(x_distribution_idx).INTERFACE_DISTRIBUTION_ID,
                                                             l_distributions_load_tbl(x_distribution_idx).INTERFACE_LINE_LOCATION_ID,
                                                             l_distributions_load_tbl(x_distribution_idx).distribution_num,
                                                             l_distributions_load_tbl(x_distribution_idx).deliver_to_location,
                                                             l_distributions_load_tbl(x_distribution_idx).requester,
                                                              l_distributions_load_tbl(x_distribution_idx).AMOUNT_ORDERED,
                                                                l_distributions_load_tbl(x_distribution_idx).SHIPPING_UOM_QUANTITY,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE1,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE2,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE3,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE4,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE5,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE6,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE7,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE8,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE9,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE10,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE12,
                                                             l_distributions_load_tbl(x_distribution_idx).ATTRIBUTE13,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT1,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT2,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT3,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT4,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT5,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT6,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT7,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT8,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT9,
                                                             l_distributions_load_tbl(x_distribution_idx).CHARGE_ACCOUNT_SEGMENT10	,
                                                             'V'
                                                             );                                                 


C_LINE_ATT_DATA:=null;
             EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;                                                 


        END;

        COMMIT; 

        dbms_output.put_line('Success');

    END LOOP;   



      EXIT WHEN l_get_prestage_txn_tbl.count < l_limit;
    END LOOP;

    CLOSE cr_get_prestage_txn;

 EXCEPTION
    WHEN OTHERS THEN
      g_error_msg := 'Unexpected Error : ' || SQLCODE || '-' || SQLERRM;
      RAISE g_main_exception;
END ;

PROCEDURE update_po ( p_oic_instance_id IN NUMBER 
                    , x_status_out OUT VARCHAR2 
                    , x_err_msg OUT VARCHAR2 
                    )
IS 

  -- CURSOR c_all_po 
  -- IS 
  -- SELECT po_number , id, po_header_id, po_amount, erp_auth_status, action_type, erp_revision_num, erp_quantity, erp_cancel_flag erp_closed_code
    -- FROM XXHTZ_EMEA_PO_INBOUND_STG_TABLE
   -- WHERE oic_instance_id = p_oic_instance_id;

  x_step VARCHAR2(1000);

  BEGIN

      x_step := 'Updating PO records not in Approved Status';

      UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
            SET record_status = 'UPDATE FAIL'
               , error_message = 'PO is NOT in Approved Status. Unable to update this PO'
               , action_type = 'NO UPDATE'
               , status = 'E'
               , po_interface_status = 'PO_UPDATE_FAILED' --added on 1 dec
           WHERE oic_instance_id = p_oic_instance_id
            AND erp_auth_status <> 'Y'
            AND po_header_id IS NOT NULL;


      x_step := 'Updating PO records for Cancel action';

      UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
            SET action_type = 'CANCEL'
              , status ='N'
           WHERE oic_instance_id = p_oic_instance_id
            AND TO_NUMBER (po_amount) = 0 AND NVL (erp_cancel_flag, 'N') = 'N'
            AND po_header_id IS NOT NULL;

      x_step := 'Updating PO records for Update action';

      UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
         SET action_type = 'UPDATE'
           , status = 'N'
       WHERE oic_instance_id = p_oic_instance_id
        AND erp_quantity <> po_amount
        AND po_amount <>0 
        AND po_header_id IS NOT NULL 
        AND erp_auth_status = 'Y'
        AND action_type is NULL;

      x_step := 'Updating PO records with same amount as Revision amount';

      UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
                 SET record_status = 'UPDATE NOT REQUIRED'
                 , error_message = 'No update required as the exisiting amount is same as Revision amount'
                 , action_type = 'NO UPDATE'
                 , status ='S'
                 , po_interface_status = 'PO_UPDATE_NOT_PROCESSED'  --added on 1 dec
                WHERE oic_instance_id = p_oic_instance_id
                 AND erp_quantity = po_amount
                 AND po_amount <>0 
                 AND po_header_id IS NOT NULL 
                AND action_type IS NULL;

      x_step := 'Updating action type as Create';
      UPDATE XXHTZ_EMEA_PO_INBOUND_STG_TABLE
                 SET action_type = 'CREATE'
                   , status ='N'
                WHERE oic_instance_id = p_oic_instance_id
                 AND action_type IS NULL
                 AND po_header_id IS NULL ;

      COMMIT;

    EXCEPTION 
    WHEN OTHERS THEN 
      x_status_out :='E';
      x_err_msg := 'Update statement failed for ' || x_step || ':-' ||SQLERRM ;
      ROLLBACK;

  END update_po;



END XXHTZ_EMEA_PO_INT_IMPORT_PKG;

/
