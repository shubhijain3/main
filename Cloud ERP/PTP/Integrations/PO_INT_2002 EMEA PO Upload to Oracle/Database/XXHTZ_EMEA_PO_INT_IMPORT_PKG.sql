--------------------------------------------------------
--  File created - Friday-November-18-2022   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Package XXHTZ_EMEA_PO_INT_IMPORT_PKG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "HTZ_CUSTOM"."XXHTZ_EMEA_PO_INT_IMPORT_PKG" AS
/*****************************************************************
OBJECT NAME: Common EMEA PO  Inbound  interface package
DESCRIPTION: Common EMEA PO Inbound   interface package

Version 	Name              	Date           		Description
----------------------------------------------------------------------------
<1.0>		Lakshman Kaigala	26-Sep-2022 	        V1.0- Initial Draft
*****************************************************************/
/*
    G_HDR_VAL_ERR_CODE 		CONSTANT VARCHAR2( 100 )DEFAULT 'HEADER_VALIDATION_ERROR';
    G_HDR_VAL_ERR_MESSAGE 	CONSTANT VARCHAR2( 100 )DEFAULT 'Invoice header validation failed';
    G_CUSTOM_ERR_TYPE 		CONSTANT VARCHAR2( 100 )DEFAULT 'Custom';
    G_HDR_IMP_ERR_CODE 		CONSTANT VARCHAR2( 100 )DEFAULT 'HEADER_IMPORT_ERROR';
    G_HDR_IMP_ERR_MESSAGE 	CONSTANT VARCHAR2( 100 )DEFAULT 'Invoice header import failed';
    G_LIN_IMP_ERR_CODE 		CONSTANT VARCHAR2( 100 )DEFAULT 'LINE_IMPORT_ERROR';
    G_LIN_IMP_ERR_MESSAGE 	CONSTANT VARCHAR2( 100 )DEFAULT 'Invoice line import failed';
    G_PARENT_TABLE_NAME 	CONSTANT VARCHAR2( 100 )DEFAULT 'xx_ap_taco_invoices_interface';
    G_LIN_VAL_ERR_CODE 		CONSTANT VARCHAR2( 100 )DEFAULT 'LINE_VALIDATION_ERROR';
    G_LIN_VAL_ERR_MESSAGE 	CONSTANT VARCHAR2( 100 )DEFAULT 'Invoice line validation failed';
    G_STANDARD_ERR_TYPE 	CONSTANT VARCHAR2( 100 )DEFAULT 'Standard'; 
	*/

/*
	TYPE deliver_to_location IS RECORD (
		deliver_to_location_code			            VARCHAR2(1000),
		oracle_deliver_to_location_id			VARCHAR2(1000)
	);

	TYPE deliver_to_location_tab IS TABLE OF deliver_to_location
	INDEX BY BINARY_INTEGER;


	TYPE bill_to_location IS RECORD (
		Vendor_num			            VARCHAR2(1000),
		oracle_bill_to_location_id				VARCHAR2(1000)
	);

	TYPE bill_to_location_tab IS TABLE OF bill_to_location
	INDEX BY BINARY_INTEGER;   


    PROCEDURE xx_update_staging_tbl (p_deliver_to_location_tab    deliver_to_location_tab,
									p_bill_to_location_tab    bill_to_location_tab,
								   p_Load_Flow_ID        		    IN   NUMBER,
							       p_status              		    OUT  VARCHAR2
								   ); */
  --  PROCEDURE XX_VALIDATE_FILE_DATA(p_file_name VARCHAR2, p_flow_id NUMBER);

	PROCEDURE XX_PO_INT_ASYNC_CALL_1(p_file_name VARCHAR2, p_flow_id NUMBER,p_job_name OUT VARCHAR2);

    PROCEDURE XX_INSERT_FBDI_DATA(p_file_name VARCHAR2, p_flow_id NUMBER,p_status OUT VARCHAR2);	

    TYPE x_error_det_rec_type IS RECORD( po_header_id 			number
									  , po_number 		VARCHAR2( 250 )
                                      , org_id 		number
									  , request_id 		number
                                      , agent_id 		number
                                      , vendor_id 		number
                                      , po_comments 		VARCHAR2( 1000 )
                                      ,interface_status VARCHAR2( 250 )
                                      ,error_code VARCHAR2( 500 )
                                      ,error_message VARCHAR2( 4000 )
									  );

    TYPE x_err_det_type IS TABLE OF x_error_det_rec_type;

   PROCEDURE xx_update_status( p_err_det_type 	IN  x_err_det_type
						   , p_error_status		IN  VARCHAR2
						   , p_success_status	IN  VARCHAR2
						   , p_oic_flow_id		IN  VARCHAR2
						   , p_total_rec_count	OUT NUMBER
						   , p_status_out       OUT VARCHAR2
						   , p_err_msg_out      OUT VARCHAR2 
						   );

   PROCEDURE update_po ( p_oic_instance_id IN NUMBER 
                    , x_status_out OUT VARCHAR2 
                    , x_err_msg OUT VARCHAR2 
                    );                           


END XXHTZ_EMEA_PO_INT_IMPORT_PKG;

/
