CREATE OR REPLACE PACKAGE BODY MPS_INTEGRATION
AS
/******************************************************************************/
/*  Package Body:      MPS_INTEGRATION
/*  SVN File:          https://javabuilder2.reed.co.uk/svn/XmsDatabaseScripts/trunk/XmsSchema-Packages/MPS_INTEGRATION_pkg_bdy.sql
/*  Last Change:       26/09/2022 Sanj Vijh
/*  Created By:        Graham Faulkner
/*  Created Date:      May 2022
/*  Description:
/*   Package containing interface code for the Metropolitan Police Service (MPS) contract.
/*   For more details, see Confluence page "MPS Interface Scope Template" (https://reedglobal.atlassian.net/wiki/spaces/X3BA/pages/2811035653/MPS+Interface+Scope+Template).
/******************************************************************************/
/*  Amendments:
/*  <Date>      <Developer>
/*    <Description>
/*  26/09/2022  Sanj Vijh
/*    > Jira PLSQL-2338 MPS HR Transactional supervisor change
/*       Supervisor ID to be extracted from pull supervisor from the additional authoriser tier 1. Within the user table and pull the ID from the company name (renamed MPS Supervisor ID) field of that user
/*       The link is via booking.additional_auth_tier1_role_id rather than the interim_booking.report_to_manager_role_id or perm_booking.report_to_manager_role_id
/*
/*  12/09/2022  Sanj Vijh
/*     > Jira plsql-2305 New Hires did not have the Supervisor ID set in the tracker table 
/*        New Hires when added to the tracker table needed the Supervisor ID set. Included a merge statement to that affect.
/*
/*  02/09/2022  Sanj Vijh
/*     > Jira PLSQL-2299
/*
/*  12/08/2022 Sanj Vijh
/*    > Jira PLSQL-2280 Alter the MPS HR transactional file name's prefix to MPS
/*    > Amend the C_CWLC_BASE_NAME and C_CWLA_BASE_NAME to have prefix of MPS rather than XMS.
/*
/*  11/08/2022 David Sharp
/*    > Jira PLSQL-2199/PLSQL-2180: minor change: file load validation in load_mps_cost_codes should take place before actual upsert to X3 application tables.
/*
/*  28/07/2022 Sanj Vijh
/*  > Jira PLSQL-2254 Blank title should be Mx within the HR transactional file
/*
/*  29/06/2022 Sanj Vijh
/*  > Jira PLSQL-1829 / PLSQL-2179 MPS HR transaction file.
/*  > XMS to MPS Tile Mappings. Correct bugs: Merge for New Hires included updated column in ON condition. Date formats for CWLA file Effective Date of Change/Leaving Date (and all dates) must be in DD-MON-YYYY format
/*
/*  22/06/2022 David Sharp
/*    > Jira PLSQL-2199/PLSQL-2180: included the file search patterns for the MPS cost code and agency files to enable these to be called from the new MPS file watcher.  
/*    > Jira PLSQL-2115 MPS Activity Codes Interface (https://reedglobal.atlassian.net/browse/PLSQL-2181)
/*
/*  21/06/2022 Graham Faulkner
/*    > Jira PLSQL-2113 MPS Position Interface (https://reedglobal.atlassian.net/browse/PLSQL-2113)
/*    > Jira PLSQL-2115 MPS Activity Codes Interface (https://reedglobal.atlassian.net/browse/PLSQL-2181)
/*
/*  21/06/2022 David Sharp
/*  >   Jira PLSQL-2114 / PLSQL-2199 MPS Cost Code file load. 
/*  >   Initial check-in for this ticket. Top-level procedure for loading MPS cost code files - load_mps_cost_codes.
/*
/*  20/06/2022 David Sharp
/*  >   Jira PLSQL-2117 / PLSQL-2180 MPS Agency Interface (i.e. MPS employee bookings).
/*  >   Initial check-in for this ticket. Top-level procedure call for loading MPS employee booking files - load_mps_emp_bkg.
/*
/*  20/06/2022 Sanj Vijh
/*  >   Jira PLSQL-1829 / PLSQL-2179 MPS HR transaction file.
/*  >   Main procedures are : 
/*  >   PopulateExtractTrackingTable - Prepares data in a global temporary table mps_hr_minimaster_gtt 
/*  >   generate_hr_extract_files  -  invokes PopulateExtractTrackingTable and uses contents of mps_hr_minimaster_gtt to generate one of 2 files types
/*  >   XMS_CWC_YYYYMMDDHH24MISS.xlsx (for New Hires) and XMS_CWLA_YYYYMMDDHH24MISS for ( Leavers or Amendments)
/*  
/*  26/05/2022 Graham Faulkner
/*    Initial revision.
/******************************************************************************/
/*  Exceptions:
/*  -20001 File type cannot be identified from file name
/*  -20410 Specified MPS employee booking file doesn't exist in file location.
/*  -20400 Unable to assign a Reed File Log ID for the specified MPS employee booking file.
/*  -20200 Specified MPS cost code file contains invalid status codes.
/*  -20100 Specified MPS cost code file doesn't exist in file location.
/*  -20500 Unable to assign a Reed File Log ID for the specified MPS cost code file.
/*  -20425 User-defined catch-all error message.
/******************************************************************************/

/******************************************************************************/
/*  Constants
/******************************************************************************/

  C_HRMINIMASTER_MPS_PROCESSING  CONSTANT NUMBER := 1;
  C_HRMINIMASTER_MPS_PROCESSED   CONSTANT NUMBER := 2;

  C_REQUEST_TYPE_NEW_HIRE        CONSTANT VARCHAR2(20) := 'New Hire';
  C_REQUEST_TYPE_AMENDMENT       CONSTANT VARCHAR2(20) := 'Amendment';
  C_REQUEST_TYPE_LEAVER          CONSTANT VARCHAR2(20) := 'Leaver';
  C_REQUEST_TYPE_EXISTING        CONSTANT VARCHAR2(20) := 'Existing';
  C_EXTRACT_DATE_FORMAT          CONSTANT VARCHAR2(20) := 'DD-MON-YYYY';
  C_TEMPORARY_GRADE              CONSTANT VARCHAR2(20) :=  'Unbanded';
  C_PERSON_TYPE                  CONSTANT VARCHAR2(20) :=  'Agency';  
  C_TITLE_GENDER_MAP             CONSTANT xms_parameters.name%TYPE        := 'MPS_XMS_TITLE_MAPPING';    
  C_PACKAGE_NAME                 CONSTANT user_objects.object_name%TYPE   := 'mps_integration';
  C_MPS_CC_TRACE_FILE_PREFIX     CONSTANT reed_file_log.file_name%TYPE    := 'plsql_2199_mps_cc_load_trace';  -- Trace log file name prefix for debugging/testing only the MPS cost code file.

/******************************************************************************/
/*  Global Variables
/******************************************************************************/
  
  g_mps_cc_load_trace_file_name  reed_file_log.file_name%TYPE;   -- MPS cost code file load trace log file name.
  
/******************************************************************************/
/*  Functions / Procedures
/******************************************************************************/

  ----------------------------------------------------------------------------------------------------
  -- Function to return the MPS org id
  ----------------------------------------------------------------------------------------------------
  FUNCTION mps_org_id
  RETURN org.org_id%TYPE
  IS
  BEGIN
    RETURN C_MPS_ORG_ID ;
  END mps_org_id ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to populate session variables.
  -- Called from initialization section, but is a public procedure and can be called if session variables need updating.
  ----------------------------------------------------------------------------------------------------
  PROCEDURE populate_session_variables
  IS
  BEGIN
    g_mps_cc_load_trace_file_name    := C_MPS_CC_TRACE_FILE_PREFIX  || TO_CHAR (SYSDATE, '_YYYY_MM_DD') || '.log' ;
  END populate_session_variables ;


  ----------------------------------------------------------------------------------------------------
  -- Function to check whether a particular file exists
  ----------------------------------------------------------------------------------------------------
  FUNCTION file_exists (
    pi_location                    IN     all_directories.directory_name%TYPE,
    pi_filename                    IN     VARCHAR2
  )
  RETURN BOOLEAN
  IS
    v_file_exists                  BOOLEAN ;
    v_file_length                  NUMBER ;
    v_block_size                   BINARY_INTEGER ;
  BEGIN
    utl_file.fgetattr (
      location     => pi_location,
      filename     => pi_filename,
      fexists      => v_file_exists,
      file_length  => v_file_length,
      block_size   => v_block_size
    ) ;

    RETURN v_file_exists ;
  END file_exists ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to move a file from one directory to another
  ----------------------------------------------------------------------------------------------------
  PROCEDURE move_file (
    pi_file_name                   IN     all_external_locations.location%TYPE,
    pi_src_location                IN     all_directories.directory_name%TYPE,
    pi_dest_location               IN     all_directories.directory_name%TYPE
  )
  IS
  BEGIN
    IF file_exists( pi_location => pi_src_location, pi_filename => pi_file_name ) = TRUE THEN
      utl_file.frename (
        src_location     => pi_src_location,
        src_filename     => pi_file_name,
        dest_location    => pi_dest_location,
        dest_filename    => pi_file_name
      ) ;
    END IF ;
  END move_file ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to archive an MPS inbound file by moving it from the inbound directory to the inbound archive directory
  ----------------------------------------------------------------------------------------------------
  PROCEDURE archive_mps_inbound_file (
    pi_file_name                   IN     all_external_locations.location%TYPE
  )
  IS
  BEGIN
    move_file (
      pi_file_name                   => pi_file_name,
      pi_src_location                => C_DIRECTORY_MPS_IN,
      pi_dest_location               => C_DIRECTORY_MPS_IN_ARC
    ) ;
  END archive_mps_inbound_file ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to load an MPS position file and merge the data into table CLIENT_CODE.
  -- MPS position file is a delta (inserts and updates) file.
  ----------------------------------------------------------------------------------------------------
  PROCEDURE load_positions (
    pi_file_name                   IN     user_external_locations.location%TYPE
  )
  IS
    C_CLIENT_CODE_TYPE_LKUP_VAL_ID CONSTANT client_code.client_code_type_lookup_val_id%TYPE := CLIENT_CODE_LOAD.C_CLI_CD_TYPE_ID_POSITION ;
    C_STATUS_ACTIVE                CONSTANT mps_position_ext.status%TYPE := 'Active' ;
    C_STATUS_INACTIVE              CONSTANT mps_position_ext.status%TYPE := 'Inactive' ;

    cur_client_codes               client_code_load.client_code_rct ;
    v_record_count                 PLS_INTEGER ;
  BEGIN
    dbms_output.put_line( 'Procedure: mps_integration.load_positions' ) ;

    IF file_exists( pi_location => C_DIRECTORY_MPS_IN, pi_filename => pi_file_name ) = TRUE THEN
      -- Change external table file location (does not invalidate code)
      EXECUTE IMMEDIATE 'ALTER TABLE MPS_POSITION_EXT LOCATION (' || dbms_assert.enquote_literal(pi_file_name) || ')' ;

      OPEN cur_client_codes FOR
        SELECT
          NULL AS client_code_id, -- populated by procedure client_code_load.load_client_codes
          C_MPS_ORG_ID AS org_id,
          C_CLIENT_CODE_TYPE_LKUP_VAL_ID AS client_code_type_lookup_val_id,
          mps_position_ext.position_id AS client_code,
          mps_position_ext.position_name AS client_code_name,
          CASE mps_position_ext.status WHEN C_STATUS_ACTIVE THEN CLIENT_CODE_LOAD.C_VALID WHEN C_STATUS_INACTIVE THEN CLIENT_CODE_LOAD.C_NOT_VALID END AS is_valid, -- any other status values will cause load to fail because is_valid is not nullable
          CASE WHEN mps_position_ext.status = C_STATUS_INACTIVE THEN COALESCE(client_code.expiry_date,TRUNC(sysdate)) END AS expiry_date, -- do not change expiry date if already expired (unless code is reactivated)
          COALESCE(client_code.created_by_xms_login_id,C_XMS_LOGIN_ID_ADMIN) AS created_by_xms_login_id,
          COALESCE(client_code.date_created,systimestamp) AS date_created,
          C_XMS_LOGIN_ID_ADMIN AS updated_by_xms_login_id,
          systimestamp AS last_updated
        FROM mps_position_ext
         LEFT OUTER JOIN client_code ON ( mps_position_ext.position_id = client_code.client_code AND client_code.org_id = C_MPS_ORG_ID AND client_code.client_code_type_lookup_val_id = C_CLIENT_CODE_TYPE_LKUP_VAL_ID )
        ;

      client_code_load.load_client_codes (
        pi_client_code_cursor          => cur_client_codes,
        po_record_count                => v_record_count
      ) ;

      dbms_output.put_line( v_record_count || ' records merged into table client_code' ) ;

      archive_mps_inbound_file( pi_file_name => pi_file_name ) ;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'File "' || pi_file_name || '" does not exist in directory "' || C_DIRECTORY_MPS_IN ) ;
    END IF ;

    COMMIT ;
  END load_positions ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to load an MPS activity code file and merge the data into table CLIENT_CODE.
  -- MPS activity code file is a delta (inserts and updates) file.
  ----------------------------------------------------------------------------------------------------
  PROCEDURE load_activity_codes (
    pi_file_name                   IN     user_external_locations.location%TYPE
  )
  IS
    C_CLIENT_CODE_TYPE_LKUP_VAL_ID CONSTANT client_code.client_code_type_lookup_val_id%TYPE := CLIENT_CODE_LOAD.C_CLI_CD_TYPE_ID_ACTIVITY_CODE ;
    C_STATUS_ACTIVE                CONSTANT mps_activity_code_ext.status%TYPE := 'Active' ;
    C_STATUS_INACTIVE              CONSTANT mps_activity_code_ext.status%TYPE := 'Inactive' ;

    cur_client_codes               client_code_load.client_code_rct ;
    v_record_count                 PLS_INTEGER ;
  BEGIN
    dbms_output.put_line( 'Procedure: mps_integration.load_activity_codes' ) ;

    IF file_exists( pi_location => C_DIRECTORY_MPS_IN, pi_filename => pi_file_name ) = TRUE THEN
      -- Change external table file location (does not invalidate code)
      EXECUTE IMMEDIATE 'ALTER TABLE MPS_ACTIVITY_CODE_EXT LOCATION (' || dbms_assert.enquote_literal(pi_file_name) || ')' ;

      OPEN cur_client_codes FOR
        SELECT
          NULL AS client_code_id, -- populated by procedure client_code_load.load_client_codes
          C_MPS_ORG_ID AS org_id,
          C_CLIENT_CODE_TYPE_LKUP_VAL_ID AS client_code_type_lookup_val_id,
          mps_activity_code_ext.code AS client_code,
          mps_activity_code_ext.description AS client_code_name,
          CASE mps_activity_code_ext.status WHEN C_STATUS_ACTIVE THEN CLIENT_CODE_LOAD.C_VALID WHEN C_STATUS_INACTIVE THEN CLIENT_CODE_LOAD.C_NOT_VALID END AS is_valid, -- any other status values will cause load to fail because is_valid is not nullable
          CASE WHEN mps_activity_code_ext.status = C_STATUS_INACTIVE THEN COALESCE(client_code.expiry_date,TRUNC(sysdate)) END AS expiry_date, -- do not change expiry date if already expired (unless code is reactivated)
          COALESCE(client_code.created_by_xms_login_id,C_XMS_LOGIN_ID_ADMIN) AS created_by_xms_login_id,
          COALESCE(client_code.date_created,systimestamp) AS date_created,
          C_XMS_LOGIN_ID_ADMIN AS updated_by_xms_login_id,
          systimestamp AS last_updated
        FROM mps_activity_code_ext
         LEFT OUTER JOIN client_code ON ( mps_activity_code_ext.code = client_code.client_code AND client_code.org_id = C_MPS_ORG_ID AND client_code.client_code_type_lookup_val_id = C_CLIENT_CODE_TYPE_LKUP_VAL_ID )
        ;

      client_code_load.load_client_codes (
        pi_client_code_cursor          => cur_client_codes,
        po_record_count                => v_record_count
      ) ;

      dbms_output.put_line( v_record_count || ' records merged into table client_code' ) ;

      archive_mps_inbound_file( pi_file_name => pi_file_name ) ;
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'File "' || pi_file_name || '" does not exist in directory "' || C_DIRECTORY_MPS_IN ) ;
    END IF ;

    COMMIT ;
  END load_activity_codes ;


  ------------------------------------------------------------------------------------------------------------------------  
  -- Procedure to insert load table, MPS_EMPLOYEE_BOOKING_GTT, with the data from the MPS_EMPLOYEE_BOOKING_GTT external table.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE insert_mps_emp_bkg_gtt ( pi_file_name         IN reed_file_log.file_name%TYPE
                                   , pi_reed_file_log_id  IN reed_file_log.reed_file_log_id%TYPE )
  IS
    -- Local constants...
    lc_process_name         CONSTANT reed_file_log.process_name%TYPE   := 'insert_mps_emp_bkg_gtt' ;
    lc_file_name            CONSTANT reed_file_log.file_name%TYPE      := COALESCE (pi_file_name, '<UNKNOWN>');

  BEGIN
    -- To do: complete this stub procedure.
    -- Clear down GTT table since its table mode is ON COMMIT PRESERVE ROWS...
    EXECUTE IMMEDIATE ( 'TRUNCATE TABLE mps_employee_booking_gtt' );

    IF NOT file_exists( pi_location => C_DIRECTORY_MPS_IN
                      , pi_filename => pi_file_name )
    THEN
      RAISE_APPLICATION_ERROR  ( num => -20410
                               , msg => UTL_LMS.FORMAT_MESSAGE ( q'[Error raised in %s.%s. File '%s' does not exist in the %s directory.]'
                                                               , C_PACKAGE_NAME
                                                               , lc_process_name
                                                               , lc_file_name
                                                               , C_DIRECTORY_MPS_IN
                                                               ));
    ELSE
      EXECUTE IMMEDIATE (UTL_LMS.FORMAT_MESSAGE ( q'[ALTER TABLE mps_employee_booking_ext LOCATION ('%s')]'
                                                , lc_file_name ));

      INSERT
      INTO mps_employee_booking_gtt
      (  file_record_number
       , psop_employee_code
       , xms_booking_id_string
       , xms_booking_id_number
       , reed_file_log_id       
         -- XMS meta data...
       , created_by_xms_login_id
       , updated_by_xms_login_id       
--       , error_message
--       , xms_booking_id_count
--       , date_created
--       , last_updated
       )
      SELECT ext.file_record_number                         AS file_record_number
           , TRIM (ext.psop_employee_code)                  AS psop_employee_code
           , TRIM (ext.xms_booking_id)                      AS xms_booking_id_string
           , TO_NUMBER(ext.xms_booking_id
             DEFAULT NULL ON CONVERSION ERROR)              AS xms_booking_id_number
           , pi_reed_file_log_id                            AS reed_file_log_id             
           , C_XMS_LOGIN_ID_ADMIN                           AS created_by_xms_login_id
           , C_XMS_LOGIN_ID_ADMIN                           AS updated_by_xms_login_id
      FROM   mps_employee_booking_ext ext;

    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE ;
  END insert_mps_emp_bkg_gtt;

  ------------------------------------------------------------------------------------------------------------------------  
  -- Function to load a blob from a file for sending as an an email attachment; 
  -- based on version of this function from  PLSQL1342_XMS_FURLOUGH_REPORT
  ------------------------------------------------------------------------------------------------------------------------  
  FUNCTION load_blob_from_file (
    pi_directory_name              IN     all_directories.directory_name%TYPE,
    pi_file_name                   IN     VARCHAR2 )
  RETURN BLOB
  IS
    v_bfile                        BFILE := bfilename( pi_directory_name, pi_file_name ) ;
    v_blob                         BLOB ;
  BEGIN
    dbms_lob.createtemporary(v_blob,true) ;
    dbms_lob.fileopen(v_bfile, dbms_lob.file_readonly) ;
    dbms_lob.loadfromfile(v_blob,v_bfile,dbms_lob.getlength(v_bfile)) ;
    dbms_lob.fileclose(v_bfile) ;
    RETURN v_blob ;
  END load_blob_from_file ;

  ------------------------------------------------------------------------------------------------------
  -- Procedure to switch on or off the trace log file for debugging/information/validation reporting.
  -- Parameters:  pi_trace_file_name    - name of file for logging to.
  --              pi_new_trace_file_dir - directory to write trace file to.
  --              pi_switch_on          - switch on|off logging to the file.
  --              pi_new_trace_file     - Boolean flag to say whether or not to create a new debug/information trace log file
  --                                    - or else to append to an existing trace log file.
  --              pi_timestamp          - flag to say whether or not to prefix each line with a timestamp.
  ------------------------------------------------------------------------------------------------------
  PROCEDURE switch_on_trace ( pi_trace_file_name    IN reed_file_log.file_name%TYPE DEFAULT ''
                            , pi_new_trace_file_dir IN reed_file_log.file_location%TYPE DEFAULT C_DIRECTORY_MPS_IN_ARC
                            , pi_switch_on          IN BOOLEAN DEFAULT TRUE
                            , pi_new_trace_file     IN BOOLEAN DEFAULT TRUE
                            , pi_timestamp          IN BOOLEAN DEFAULT TRUE )
  IS
  BEGIN
    IF pi_switch_on
    THEN
      IF pi_new_trace_file
      THEN
       -- Delete previous trace file if it exists...
        BEGIN
          reed_dsp.delete_file ( p_dir    => pi_new_trace_file_dir
                               , p_file   => pi_trace_file_name );
        EXCEPTION
        WHEN OTHERS THEN
          NULL;
        END;

        reed_dsp.show_output_on; -- Logger information: turn on the trace.
        reed_dsp.line_wrap_on;
        reed_dsp.set_max_width (1000);
        reed_dsp.file_output_on ( p_file_dir  => pi_new_trace_file_dir
                               ,  p_file_name => pi_trace_file_name );

        IF pi_timestamp
        THEN
          reed_dsp.line (' ');  -- Newline
          reed_dsp.line ('-----------------------------------------------------------------------------------');
        END IF;

      ELSE -- appending to existing file
        reed_dsp.show_output_on; -- Logger information: turn on the trace.

        reed_dsp.line_wrap_on;
        reed_dsp.set_max_width (1000);

        reed_dsp.file_output_on ( p_file_dir  => pi_new_trace_file_dir
                               ,  p_file_name => pi_trace_file_name );
      END IF;

      --reed_dsp.line (' ');  -- Newline
      IF pi_timestamp
      THEN
        -- Prefix the messages with a timestamp.
        reed_dsp.show_date_on;
        --
      ELSE
        reed_dsp.show_date_off;
      END IF;
    ELSE -- switch off tracing
      IF pi_timestamp
      THEN
        reed_dsp.show_date_off;
        reed_dsp.line ('-----------------------------------------------------------------------------------');
      END IF;
      reed_dsp.show_output_off;
    END IF ;
  END switch_on_trace;  
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Function to get recipient list for the MPS employee booking file data validation email; returned as ref cursor type.
  ------------------------------------------------------------------------------------------------------------------------  
  FUNCTION get_mps_emp_bkg_err_rep_recipients_rc
  RETURN   reed_xms_email_queue.recipients_ct  
  IS
    -- Local constants...  
    lc_xms_type_lkup_val_id_person  CONSTANT comms_recipients.xms_type_lookup_val_id%TYPE := 3    ; -- Person-type XMS-type lookup value for recipients that exist in XMS
    lc_xms_type_lookup_id           CONSTANT lookup.lookup_id%TYPE                        := 1    ; -- XMS_TYPE -- defines all the available XMS types
    lc_xms_type_lkup_val_id_default CONSTANT comms_recipients.xms_type_lookup_val_id%TYPE := 4653 ; -- Comms broadcast-type XMS-type lookup value - used for for recipients that don't exist in XMS

    -- Local variables...
    l_recipient_list_rc             reed_xms_email_queue.recipients_ct ;
  BEGIN

    OPEN l_recipient_list_rc
    FOR
      SELECT gla_integration.gla_implementation_email     AS email
           , lc_xms_type_lookup_id                        AS xms_id  -- Email utility needs a valid xms_id for the recipient, so default to 1 if recipient not set up on XMS.
           , lc_xms_type_lkup_val_id_person               AS xms_type_lookup_val_id
           , NULL                                         AS template_object_id
           , NULL                                         AS template_object_type
      FROM   DUAL;
    RETURN l_recipient_list_rc;

  EXCEPTION
    WHEN OTHERS THEN
      RAISE ;
  END get_mps_emp_bkg_err_rep_recipients_rc;
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Function to get any MPS employee booking file load validation warnings/errors encountered as
  -- part of the MPS file load, and to return a list of consolidated warning|error-type data validation issue(s) 
  -- per each record in the file.
  -- Returned as a ref cursor (rc) - and made public - to help with testing and also cursor re-use.
  ------------------------------------------------------------------------------------------------------------------------  
  FUNCTION get_mps_emp_bkg_err_rc ( pi_reed_file_log_id IN reed_file_log.reed_file_log_id%TYPE )
  RETURN   mps_emp_bkg_err_rct
  IS
    -- Local Ref Cursors ...
    l_mps_emp_bkg_err_rc  mps_emp_bkg_err_rct;

  BEGIN

    OPEN l_mps_emp_bkg_err_rc
    FOR
      WITH mps_file_sq
      AS
        (
          SELECT    mps.file_record_number              AS file_record_number
                  , mps.psop_employee_code              AS psop_employee_code
                  , mps.xms_booking_id_string           AS xms_booking_id_string
                  , mps.xms_booking_id_number           AS xms_booking_id_number
                  , COUNT(*) OVER
                             (
                                PARTITION BY xms_booking_id_string
                             )                          AS xms_booking_count
                  , COUNT(*) OVER
                             (
                                PARTITION BY psop_employee_code
                             )                          AS psop_employee_count
                 , mps.reed_file_log_id                 AS reed_file_log_id
          FROM     mps_employee_booking_gtt mps
          WHERE    mps.reed_file_log_id  = pi_reed_file_log_id
        )
      , mps_file_errs_sq
      AS
        (  SELECT -- Duplicate XMS bookings in the same MPS file - an error condition.  These rows will be rejected...
                  mps_file_sq.file_record_number             AS file_record_number
                , mps_file_sq.psop_employee_code             AS psop_employee_code
                , mps_file_sq.xms_booking_id_string          AS xms_booking_id_string
                , 'Error: duplicate XMS bookings in the same MPS Agency file'
                                                             AS error_message
                , 'N'                                        AS valid_record_yn
                , pi_reed_file_log_id                        AS reed_file_log_id
           FROM   mps_file_sq
           WHERE  1=1
           AND    xms_booking_id_string IS NOT NULL                       
           AND    xms_booking_count > 1
           UNION
           SELECT -- Duplicate MPS PSOP employee ID's in the same MPS file - a warning condition. These rows will still be processed but reported...
                  mps_file_sq.file_record_number             AS file_record_number
                , mps_file_sq.psop_employee_code             AS psop_employee_code
                , mps_file_sq.xms_booking_id_string          AS xms_booking_id_string
                , 'Warning: duplicate MPS Warrant numbers in the same MPS Agency file'
                                                             AS error_message
                , 'Y'                                        AS valid_record_yn
                , pi_reed_file_log_id                        AS reed_file_log_id
           FROM   mps_file_sq
           WHERE  1=1
           AND    psop_employee_code IS NOT NULL            
           AND    psop_employee_count > 1
           UNION
           SELECT -- Any supplied XMS bookings not booked under the MPS org (whether soft-deleted or not)...
                  mps_file_sq.file_record_number            AS file_record_number
                , mps_file_sq.psop_employee_code            AS psop_employee_code
                , mps_file_sq.xms_booking_id_string         AS xms_booking_id_string
                , 'Error: XMS booking is booked under the '|| TRIM (org.name) || ' org instead of the MPS org'
                                                            AS error_message
                , 'N'                                       AS valid_record_yn
                , pi_reed_file_log_id                       AS reed_file_log_id
           FROM   mps_file_sq
             INNER JOIN booking ON mps_file_sq.xms_booking_id_number = booking.booking_id
             INNER JOIN item    ON booking.item_id = item.item_id
             INNER JOIN orders  ON item.order_id   = orders.order_id
             INNER JOIN org     ON org.org_id      = orders.org_id
           WHERE orders.org_id <> C_MPS_ORG_ID
           UNION
           SELECT -- NULL MPS PSOP Code (i.e. the MPS system employee ID). Error condition...
                  mps_file_sq.file_record_number            AS file_record_number
                , mps_file_sq.psop_employee_code            AS psop_employee_code
                , mps_file_sq.xms_booking_id_string         AS xms_booking_id_string
                , 'Error: no value provided for the MPS Warrant number'
                                                            AS error_message
                , 'N'                                       AS valid_record_yn
                , pi_reed_file_log_id                       AS reed_file_log_id
           FROM   mps_file_sq
           WHERE  mps_file_sq.psop_employee_code IS NULL
           UNION
           SELECT -- NULL XMS Booking ID provided. Error condition...
                  mps_file_sq.file_record_number            AS file_record_number
                , mps_file_sq.psop_employee_code            AS psop_employee_code
                , mps_file_sq.xms_booking_id_string         AS xms_booking_id_string
                , 'Error: no value provided for the XMS Booking ID'
                                                            AS error_message
                , 'N'                                       AS valid_record_yn
                , pi_reed_file_log_id                       AS reed_file_log_id
           FROM   mps_file_sq
           WHERE  mps_file_sq.xms_booking_id_string IS NULL
           UNION
           SELECT -- XMS Booking ID contains non-numerics. Error condition...
                  mps_file_sq.file_record_number            AS file_record_number
                , mps_file_sq.psop_employee_code            AS psop_employee_code
                , mps_file_sq.xms_booking_id_string         AS xms_booking_id_string
                , 'Error: the XMS Booking ID contains non-numerics'
                                                            AS error_message
                , 'N'                                       AS valid_record_yn
                , pi_reed_file_log_id                       AS reed_file_log_id
           FROM   mps_file_sq
           WHERE  mps_file_sq.xms_booking_id_string IS NOT NULL
           AND    mps_file_sq.xms_booking_id_number IS NULL
        )
     , mps_err_list_sq
       AS
        (  -- Consolidated list of MPS employee booking file data validation issues per file record...
          SELECT mps_file_errs_sq.file_record_number          AS file_record_number
               , mps_file_errs_sq.psop_employee_code          AS psop_employee_code
               , mps_file_errs_sq.xms_booking_id_string       AS xms_booking_id_string
               , LISTAGG (mps_file_errs_sq.error_message, '; ' || CHR(10) ON OVERFLOW TRUNCATE '...' WITHOUT COUNT)
                   WITHIN GROUP (ORDER BY mps_file_errs_sq.error_message) || '.'
                                                              AS error_message_list
               , MIN (mps_file_errs_sq.valid_record_yn)       AS valid_record_yn
               , mps_file_errs_sq.reed_file_log_id            AS reed_file_log_id
          FROM   mps_file_errs_sq
          GROUP BY mps_file_errs_sq.file_record_number
                 , mps_file_errs_sq.psop_employee_code
                 , mps_file_errs_sq.xms_booking_id_string
                 , mps_file_errs_sq.reed_file_log_id
          ORDER BY mps_file_errs_sq.file_record_number
        )
      -- Final MPS employee booking file data validation report SQL...
      SELECT  mps_err_list_sq.file_record_number        AS "MPS File Record #"
            , mps_err_list_sq.psop_employee_code        AS "MPS PSOP Employee ID"
            , mps_err_list_sq.xms_booking_id_string     AS "XMS Booking ID"
            , mps_err_list_sq.error_message_list        AS "Data Validation Issues"
            , mps_err_list_sq.valid_record_yn           AS "Valid file record?"            
            , mps_err_list_sq.reed_file_log_id          AS "Reed File Log ID"
      FROM    mps_err_list_sq ;

    RETURN l_mps_emp_bkg_err_rc;
    
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END get_mps_emp_bkg_err_rc;

  ------------------------------------------------------------------------------------------------------------------------  
  -- Validate the MPS employee booking file for any invalid data records. (e.g. duplicate XMS booking ID's.)
  -- Only records deemed valid to be loaded into XMS.
  --
  -- po_number_of_issues: count of MPS employee booking file records that have validation issue(s).
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE validate_mps_emp_bkg_file ( pi_reed_file_log_id     IN reed_file_log.reed_file_log_id%TYPE
                                      , po_number_of_issues     OUT PLS_INTEGER )
  IS
    -- Local constants...
    lc_process_name       CONSTANT reed_file_log.process_name%TYPE := 'validate_mps_emp_bkg_file' ;

    -- Local variables...
    l_no_of_rows_updated  PLS_INTEGER := 0;
    
    -- Local Ref Cursors ...
    l_mps_emp_bkg_err_rc  mps_emp_bkg_err_rct;

    -- Local arrays...
    l_mps_emp_bkg_err_aa  mps_emp_bkg_err_aat;

    -- Bulk exceptions error...
--    bulk_errors_exc             EXCEPTION ;
--    PRAGMA EXCEPTION_INIT     ( bulk_errors_exc, -24381 );

  BEGIN
    -- Fetch the ref cursor of any file errors/warnings in the MPS employee booking file...
    l_mps_emp_bkg_err_rc := get_mps_emp_bkg_err_rc ( pi_reed_file_log_id => pi_reed_file_log_id ) ;

   LOOP
      FETCH l_mps_emp_bkg_err_rc
      BULK  COLLECT
      INTO  l_mps_emp_bkg_err_aa 
      LIMIT C_BULK_LIMIT ;

      EXIT WHEN l_mps_emp_bkg_err_aa.COUNT = 0;

      FORALL i IN 1..l_mps_emp_bkg_err_aa.COUNT
        MERGE
        INTO  mps_employee_booking_gtt tgt
          USING
          (
            SELECT  l_mps_emp_bkg_err_aa(i).reed_file_log_id      AS reed_file_log_id
                  , l_mps_emp_bkg_err_aa(i).file_record_number    AS file_record_number
                  , l_mps_emp_bkg_err_aa(i).error_message_list    AS error_message_list
                  , l_mps_emp_bkg_err_aa(i).valid_record_yn       AS valid_record_yn
            FROM   dual ) src
        ON   (    src.reed_file_log_id   = tgt.reed_file_log_id
              AND src.file_record_number = tgt.file_record_number )
      WHEN MATCHED THEN
        UPDATE
        SET   tgt.error_message             = src.error_message_list
            , tgt.valid_record_yn           = src.valid_record_yn
              -- Meta-data columns...
            , tgt.updated_by_xms_login_id   = C_XMS_LOGIN_ID_ADMIN
            , tgt.last_updated              = SYSTIMESTAMP ;
            
      l_no_of_rows_updated   := l_no_of_rows_updated + SQL%ROWCOUNT;            
      
    END LOOP ;

    CLOSE l_mps_emp_bkg_err_rc;
    
    IF l_no_of_rows_updated > 0
    THEN
      INSERT 
      INTO mps_employee_booking_err 
         ( reed_file_log_id
         , file_record_number
         , error_message
         , psop_employee_code
         , xms_booking_id
         , date_created
         , created_by_xms_login_id
         , last_updated
         , updated_by_xms_login_id )
      SELECT reed_file_log_id
           , file_record_number
           , error_message
           , psop_employee_code    AS psop_employee_code
           , xms_booking_id_string AS xms_booking_id
           , date_created
           , created_by_xms_login_id
           , last_updated
           , updated_by_xms_login_id 
      FROM   mps_employee_booking_gtt 
      WHERE  error_message IS NOT NULL;

    END IF ;
    
    po_number_of_issues := l_no_of_rows_updated;
    
  EXCEPTION
    WHEN OTHERS THEN
      IF l_mps_emp_bkg_err_rc%ISOPEN
      THEN
        CLOSE l_mps_emp_bkg_err_rc ;
      END IF ;
      RAISE ;

  END validate_mps_emp_bkg_file ;
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Procedure to log a new file into the REED_FILE_LOG table and return the unique Reed file number for this file.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE log_new_file  ( pi_result              IN  SYS.SCHEDULER_FILEWATCHER_RESULT
                          , pi_process_name        IN  reed_file_log.process_name%TYPE
                          , po_reed_file_number    OUT reed_file_log.reed_file_log_id%TYPE )
  IS
    PRAGMA AUTONOMOUS_TRANSACTION ;
    
    lc_file_name           reed_file_log.file_name%TYPE              := COALESCE ( pi_result.actual_file_name, '<UNKNOWN>' );
    l_reed_file_number     reed_file_log.reed_file_log_id%TYPE;

  BEGIN

    INSERT
    INTO  reed_file_log
        ( file_name
        , file_location
        , file_timestamp
        , process_name )
    VALUES
         ( lc_file_name
         , COALESCE ( pi_result.directory_path, '<UNKNOWN>' )
         , COALESCE ( pi_result.file_timestamp,  TO_TIMESTAMP('01/01/1900', 'DD/MM/YYYY')) -- If no file timestamp, I've set it to a default dummy 'epoc' date; I may change this to nullable but prefer to make nullable where nearly all values are not null.
         , COALESCE ( pi_process_name, '<UNKNOWN>' ))
    RETURNING reed_file_log_id
    INTO      l_reed_file_number ;

    po_reed_file_number := l_reed_file_number;

    COMMIT; -- Autonomous txn

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK; -- Autonomous txn
      RAISE ;
  END log_new_file;

  ------------------------------------------------------------------------------------------------------------------------  
  -- Procedure to update X3 bookings with the valid MPS employee booking file load data.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE  update_mps_emp_bkg ( pi_reed_file_log_id  IN reed_file_log.reed_file_log_id%TYPE )
  IS
   -- Local constants...
   
   -- To do: may not need lc_process_name below. 
    lc_process_name  CONSTANT reed_file_log.process_name%TYPE   := 'update_mps_emp_bkg' ;
    
  BEGIN
  
    MERGE
    INTO  booking tgt  
      USING
      (  
        SELECT       gtt.xms_booking_id_number    AS booking_id
                   , gtt.psop_employee_code       AS client_identifier
        FROM         mps_employee_booking_gtt  gtt
          INNER JOIN booking on gtt.xms_booking_id_number = booking.booking_id
          INNER JOIN item    on booking.item_id           = item.item_id
          INNER JOIN orders  on item.order_id             = orders.order_id
        WHERE 1=1
        AND   gtt.reed_file_log_id   = pi_reed_file_log_id
        AND   orders.org_id          = C_MPS_ORG_ID  
        AND   COALESCE (gtt.valid_record_yn, 'Y') = 'Y'  -- File records that haven't been stamped as invalid by the validation processing.        
        -- AND COALESCE (booking.marked_deleted, 'N') = 'Y') --  not required as will update booking even if they've been soft-deleted.
       ) src    
    ON  ( src.booking_id   = tgt.booking_id )
      WHEN MATCHED THEN
        UPDATE
        SET   tgt.client_identifier         = src.client_identifier
              -- Meta-data columns...
            , tgt.updated_by_xms_login_id   = C_XMS_LOGIN_ID_ADMIN
            , tgt.last_updated              = SYSTIMESTAMP ;
                  
  EXCEPTION
    WHEN OTHERS THEN    
      RAISE ;
   
  END update_mps_emp_bkg ;
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Procedure to generate an xlsx report file of MPS employee booking file validation errors.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE generate_mps_emp_bkg_err_report (  pi_err_report_file_name  IN reed_file_log.file_name%TYPE
                                             , pi_reed_file_log_id      IN reed_file_log.reed_file_log_id%TYPE
                                             , pi_number_of_issues      IN PLS_INTEGER )
  IS
    -- Local Ref Cursors ...
    l_mps_emp_bkg_err_rc           mps_emp_bkg_err_rct;
    
    l_rc                           SYS_REFCURSOR ;

    -- Local variables...
    l_file_handle                  UTL_FILE.FILE_TYPE ;

    l_sheet_number                 PLS_INTEGER := 0 ;

    -- Local arrays...
    l_mps_emp_bkg_file_err_aa      mps_emp_bkg_err_aat ;

  BEGIN      
    -- Fetch the ref cursor of MPS employee booking file validation errors to be used for the report...
    l_mps_emp_bkg_err_rc := get_mps_emp_bkg_err_rc ( pi_reed_file_log_id => pi_reed_file_log_id ) ;    
  
    reed_xlsx.clear_workbook;
    
    l_sheet_number := l_sheet_number + 1 ;
    reed_xlsx.new_sheet('MPS emp booking file - issues') ;
    
    -- Format rows (including header row)
    FOR i IN 1 .. pi_number_of_issues + 1 -- Include header row, so one more than the total number of validation issues.
    LOOP
      reed_xlsx.set_row (
        p_row       => i
      , p_fontId    => reed_xlsx.get_font( p_name        => 'Calibri'
                                         , p_fontsize    => 10
                                         , p_bold        => CASE WHEN i = 1 THEN TRUE END
                                         , p_rgb         => CASE WHEN i = 1 THEN 'FF1F497D' END ) -- p_rgb is an RGBA value. The 2 most significant digits are the alpha value that specifies transparency/opacity as a percentage (00 = fully transparent, FF = fully opaque).
      , p_alignment => reed_xlsx.get_alignment( p_vertical => 'center' )
      ) ;
    END LOOP ;

    reed_xlsx.set_autofilter (
        p_column_start  => 1
      , p_column_end    => 6
      , p_row_start     => 1
      , p_row_end       => 1
    ) ;

    reed_xlsx.freeze_rows    ( p_nr_rows =>  1
                             , p_sheet   => l_sheet_number );
    -- reed_xlsx.freeze_pane( p_col => 13
                         -- , p_row => 1 ) ;


    -- 1. Generate MPS employee booking errors worksheet from the ref cursor of errors...
    reed_xlsx.query2sheet ( cur        => l_mps_emp_bkg_err_rc
                          , p_sheet    => l_sheet_number
                          , p_usexf    => TRUE ) ;    
                                                    

    -- 2. Now start on the next worksheet in this report - i.e. the original MPS employee bokking file data...
    l_sheet_number := l_sheet_number + 1 ;    
    reed_xlsx.new_sheet('MPS emp booking file - original') ;
    
    -- Format rows (including header row)
    FOR i IN 1 .. pi_number_of_issues + 1 -- Include header row, so one more than the total number of validation issues.
    LOOP
      reed_xlsx.set_row (
        p_row       => i
      , p_fontId    => reed_xlsx.get_font( p_name        => 'Calibri'
                                         , p_fontsize    => 10
                                         , p_bold        => CASE WHEN i = 1 THEN TRUE END
                                         , p_rgb         => CASE WHEN i = 1 THEN 'FF1F497D' END ) -- p_rgb is an RGBA value. The 2 most significant digits are the alpha value that specifies transparency/opacity as a percentage (00 = fully transparent, FF = fully opaque).
      , p_alignment => reed_xlsx.get_alignment( p_vertical => 'center' )
      ) ;
    END LOOP ;

    reed_xlsx.set_autofilter (
        p_column_start  => 1
      , p_column_end    => 3
      , p_row_start     => 1
      , p_row_end       => 1
    ) ;

    reed_xlsx.freeze_rows    ( p_nr_rows =>  1
                             , p_sheet   => l_sheet_number );    

    -- Fetch the ref cursor of MPS employee booking file validation errors again to help identify the original
    -- MPS employee booking file records with validation issues...
    l_mps_emp_bkg_err_rc := get_mps_emp_bkg_err_rc ( pi_reed_file_log_id => pi_reed_file_log_id ) ;

    FETCH l_mps_emp_bkg_err_rc
    BULK  COLLECT
    INTO  l_mps_emp_bkg_file_err_aa ;
        
    -- Ref cursor of original MPS employee booking file records for those records with data validation issues...
    OPEN l_rc
    FOR
      WITH errs_sql
      AS
        (
          SELECT file_record_number                 AS file_record_number
          FROM   TABLE ( l_mps_emp_bkg_file_err_aa )
        )
      SELECT load.file_record_number                AS "MPS File Record #"    
          ,  load.psop_employee_code                AS "PSOP Employee ID"
          ,  load.xms_booking_id_string             AS "XMS Booking ID"
      FROM   mps_employee_booking_gtt load
      WHERE  load.reed_file_log_id = pi_reed_file_log_id
      AND    EXISTS ( SELECT NULL
                      FROM   errs_sql
                      WHERE  load.file_record_number = errs_sql.file_record_number )
      ORDER BY load.file_record_number ASC ;

    -- Generate the worksheet of orginal MPS emp booking file records with data validation issues...
    reed_xlsx.query2sheet ( cur        => l_rc
                          , p_sheet    => l_sheet_number
                          , p_usexf    => TRUE ) ;

    reed_xlsx.save( p_directory => C_DIRECTORY_MPS_IN_ARC
                  , p_filename  => pi_err_report_file_name );   

  EXCEPTION
    WHEN OTHERS THEN
      IF UTL_FILE.IS_OPEN ( l_file_handle)
      THEN
        UTL_FILE.FCLOSE( l_file_handle) ;
      END IF ;
      RAISE;                  
  END generate_mps_emp_bkg_err_report ;
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Create and send an MPS employee booking file load data validation xlsx error report to the user list, lc_recipient_list.
  -- This will be only send an email if there are any validation errors for this MPS file.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE create_mps_emp_bkg_err_email ( pi_mps_emp_booking_file_name IN reed_file_log.file_name%TYPE
                                         , pi_error_file_name           IN reed_file_log.file_name%TYPE
                                         , pi_email_report              IN BOOLEAN DEFAULT TRUE )
  IS
    aa_attachments    reed_xms_email_queue.attachments_aat ;
    aa_recipients     reed_xms_email_queue.recipients_aat ;
    r_recipient       reed_xms_email_queue.recipients_ct ;  -- Ref cursor type

    -- Local constants...
    lc_process_name                 CONSTANT reed_file_log.process_name%TYPE := 'create_mps_emp_bkg_err_email' ;

    lc_system_comm_typ_lv_id        CONSTANT lookup_value.lookup_val_id%TYPE := 621441; -- systems processing-type seeded data communcation email type.

    lc_email_template_text          CONSTANT comms_email.message_body%TYPE := -- comms_email.message_body is a CLOB.
    q'[<html>
    Please find attached the MPS employee booking data validation report, '%s', for the XMS load of file, '%s'.
    <br><br>This report lists all data validations issues encountered while loading this file into XMS.
    These issues are classified as either warning or error-type data validation issues.
    <br><br>Regards<br>
    IT Development<br>
    Reed Specialist Recruitment<br>
    120 Coombe Lane, Raynes Park, London, SW20 0BA<br>
    </html>]';

  BEGIN

    -- Load MPS employee booking errors report file into a blob for emailing...
    aa_attachments(1).attachment_data  := load_blob_from_file ( pi_directory_name => C_DIRECTORY_MPS_IN_ARC
                                                              , pi_file_name      => pi_error_file_name );
    aa_attachments(1).attachment_name  := pi_error_file_name ;

    r_recipient                        := get_mps_emp_bkg_err_rep_recipients_rc ; 

    IF pi_email_report
    THEN
      -- Send email to each addressee...
      reed_xms_email_queue.create_email ( pi_email_comm_template_id    => NULL
                                        , pi_subject                   => UTL_LMS.FORMAT_MESSAGE ( q'[%s: MPS employee booking file load - data validation report]'
                                                                                                 , COALESCE ( C_DB_ENVIRONMENT, '<Unknown>'))
                                        , pi_message_body              => UTL_LMS.FORMAT_MESSAGE ( lc_email_template_text
                                                                                                 , COALESCE ( pi_error_file_name, '<NULL>')
                                                                                                 , COALESCE ( pi_mps_emp_booking_file_name, '<NULL>'))
                                        , pi_comm_type_lookup_val_id   => lc_system_comm_typ_lv_id
                                        , pi_object_type               => ''
                                        , pi_attachments               => aa_attachments
                                        , pi_created_by_xms_login_id   => C_XMS_LOGIN_ID_ADMIN
                                        , pi_emailfrom                 => 'it.development@reed.com'
                                        , pi_display_sender            => 'IT Development'
                                        , pi_email_recipients_cursor   => r_recipient
                                        , pi_include_unsubscribe_link  => FALSE );

    END IF ;

    EXCEPTION
      WHEN OTHERS THEN
    RAISE ;

  END create_mps_emp_bkg_err_email;
    
  ------------------------------------------------------------------------------------------------------
  -- *** Top-level procedure for loading MPS employee bookings. *** 
  -- Created as part of Jira epic PLSQL-2117 (ticket PLSQL-2180)
  -- to load MPS employee bookings from the Met Police Single Operating System (PSOP) Finance system. 
  --
  -- pi_email_report: boolean to say whether or not the file load data validation report should be
  -- emailed to the recipient list. Note: an email will only be sent if there are any data validation issues.
  ------------------------------------------------------------------------------------------------------
  PROCEDURE load_mps_emp_bkg ( pi_file_name     IN user_external_locations.location%TYPE
                             , pi_email_report  IN BOOLEAN DEFAULT TRUE )  
  IS
    -- Local constants...
    lc_process_name          CONSTANT reed_file_log.process_name%TYPE  := 'load_mps_emp_bkg' ;
    lc_file_name             CONSTANT reed_file_log.file_name%TYPE     := COALESCE ( pi_file_name, '<UNKNOWN>' );
    lc_err_report_file_name  CONSTANT reed_file_log.file_name%TYPE     := REPLACE (lc_file_name, '.csv', '_data_validation_report.xlsx');

    -- Local variables...
    l_reed_file_log_id       reed_file_log.reed_file_log_id%TYPE ;    
    l_number_of_issues       PLS_INTEGER := 0; -- Number of data validation issues discovered in the MPS employee booking file.
    l_result_obj             SYS.SCHEDULER_FILEWATCHER_RESULT
                         :=  SYS.SCHEDULER_FILEWATCHER_RESULT  ( NULL          -- destination
                                                               , C_DIRECTORY_MPS_IN
                                                                               -- file path
                                                               , lc_file_name  -- Actual file name
                                                               , 0             -- file size
                                                               , SYSTIMESTAMP  -- file t/stamp
                                                               , 0             -- ts_ms_from_epoch
                                                               , NULL );       -- matching_requests
    -- Local exceptions...
    exc_null_file_log_id    EXCEPTION;

  BEGIN

    -- populate_session_variables; -- To do: complete this if needed?

    -- Log file load into REED_FILE_LOG and get the reed file log number for this load...
    log_new_file ( pi_result             => l_result_obj
                 , pi_process_name       => lc_process_name
                 , po_reed_file_number   => l_reed_file_log_id );


    IF l_reed_file_log_id IS NULL
    THEN
      -- Would need to explore why this is NULL so raise an error.
      RAISE exc_null_file_log_id ;
    END IF ;

    -- Insert into load table, MPS_EMPLOYEE_BOOKING_GTT that will be used to carry out
    -- data validation on the MPS file...
    insert_mps_emp_bkg_gtt ( pi_file_name          => lc_file_name
                           , pi_reed_file_log_id   => l_reed_file_log_id );

    -- Check the MPS employee booking file for any invalid records.
    -- Only records deemed valid to be loaded into XMS...
    validate_mps_emp_bkg_file ( pi_reed_file_log_id     => l_reed_file_log_id
                              , po_number_of_issues     => l_number_of_issues );

    COMMIT; -- Commit the MPS_EMPLOYEE_BOOKING_GTT data. 
    
    IF l_number_of_issues > 0
    THEN
      -- Generate a report file of MPS employee booking validation errors...
      generate_mps_emp_bkg_err_report ( pi_err_report_file_name   => lc_err_report_file_name
                                      , pi_reed_file_log_id       => l_reed_file_log_id
                                      , pi_number_of_issues       => l_number_of_issues );

      -- Send out MPS employee booking error report email(s)...
      create_mps_emp_bkg_err_email ( pi_mps_emp_booking_file_name => lc_file_name
                                   , pi_error_file_name           => lc_err_report_file_name
                                   , pi_email_report              => pi_email_report );
    END IF ;

    -- Update X3 bookings with the valid MPS employee booking file load data...
    update_mps_emp_bkg ( pi_reed_file_log_id   => l_reed_file_log_id );

    COMMIT; -- main MPS employee booking load transaction.

    -- Archive successfully loaded file...
    archive_mps_inbound_file ( pi_file_name => lc_file_name ) ;

  EXCEPTION

    -- Unable to assign Reed file Log ID number during MPS employee booking file load...
    WHEN exc_null_file_log_id 
    THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR  ( num => -20400
                               , msg => UTL_LMS.FORMAT_MESSAGE ( q'[Error raised in %s.%s loading MPS employee booking file '%s'. Unable to assign a Reed File Log ID for the '%s' file load.]'
                                                               , C_PACKAGE_NAME
                                                               , lc_process_name
                                                               , lc_file_name
                                                               ));
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE ;
  END load_mps_emp_bkg ;

  ------------------------------------------------------------------------------------------------------
  
  FUNCTION get_mps_cost_code_hierarchy_rc ( pi_record_type IN VARCHAR2 DEFAULT '' )
  RETURN   cost_code_hierarchy_rct
  IS 
    -- Local variables...
    l_cost_code_hierarchy_rc cost_code_hierarchy_rct ;

  BEGIN
    OPEN l_cost_code_hierarchy_rc
    FOR
      WITH mps_cc_file_sq
      AS
        ( -- MPS cost code file for loading into XMS...
          SELECT TRIM (ext.cost_code)           AS config_name  -- Assumed this mapping based on live sample MPS config names as at time of writing.
               , TRIM (ext.cost_code)           AS unique_id  -- To do: assume that ext.cost_code maps to the cc.unique_id too since the data in live looks (mostly) like this.            
               , NULL                           AS config_id  -- will be derived later.
               , UPPER(TRIM (ext.status))       AS status -- ACTIVE|INACTIVE.
               , TRIM (ext.level2_name)         AS level1 -- Cost Centre Level 2 in MPS file <=> Level 1 in XMS (as per confluence page, https://reedglobal.atlassian.net/wiki/spaces/X3BA/pages/2811035653/MPS+Interfaces)
               , TRIM (ext.level3_name)         AS level2 -- Cost Centre Level 3 in MPS file <=> Level 2 in XMS.
               , TRIM (ext.level4_name)         AS level3 -- Cost Centre Level 4 in MPS file <=> Level 3 in XMS.              
               , TRIM (ext.level5_name)         AS level4 -- Cost Centre Level 5 in MPS file <=> Level 4 in XMS.              
          FROM   mps_cost_code_ext ext
          WHERE  ext.cost_code IS NOT NULL -- To do: validation on this field?
                                           -- This maps to cost_code.unique_id which is nullable in x3 but think it 
                                           -- shouldn't as all the data is not null so this is to stop nulls in file being loaded into X3 if nulls exist in file.
        )      
      , mps_cc_in_xms_sq
      AS
        ( -- All live (i.e. not deleted) MPS Org cost codes in XMS...
          SELECT   TRIM (cfg.config_name) AS config_name
                 , TRIM (cc.unique_id)    AS unique_id
                 , cc.cost_cd_config_id   AS config_id
                 , TRIM (cc.level1)       AS level1  
                 , TRIM (cc.level2)       AS level2  
                 , TRIM (cc.level3)       AS level3  
                 , TRIM (cc.level4)       AS level4  
          FROM         cost_code       cc
            INNER JOIN configuration   cfg ON cc.cost_cd_config_id = cfg.config_id
            INNER JOIN org             org ON org.org_id           = cfg.org_id
          WHERE 1=1
          AND COALESCE (cc.marked_deleted,  'N') = 'N'
          AND COALESCE (cfg.marked_deleted, 'N') = 'N'
          AND COALESCE (org.marked_deleted, 'N') = 'N'
          AND cfg.config_type_lookup_val_id      = C_COST_CODE_LOOKUP_VAL_ID
          AND org.org_id                         = C_MPS_ORG_ID
        )
      , new_mps_cc_sq
      AS
        ( -- New MPS file cost codes: these don't already exist as active cost codes in XMS...
          SELECT 'New'                           AS record_type
--               , status                          AS status     
               , mps_cc_file_sq.config_name      AS config_name
               , NULL                            AS config_id -- Will derive this value later.
               , mps_cc_file_sq.unique_id        AS unique_id
               , mps_cc_file_sq.level1           AS level1
               , mps_cc_file_sq.level2           AS level2
               , mps_cc_file_sq.level3           AS level3
               , mps_cc_file_sq.level4           AS level4
          FROM   mps_cc_file_sq
          WHERE  NOT EXISTS ( SELECT NULL
                              FROM   mps_cc_in_xms_sq
                              WHERE  mps_cc_file_sq.unique_id = mps_cc_in_xms_sq.unique_id )
          AND    mps_cc_file_sq.status = 'ACTIVE' 
        )
      , existing_mps_cc_sq
      AS
        ( -- Existing MPS valid cost centre file cost codes: these match existing active cost codes in XMS...
          SELECT 'Existing'                      AS record_type
               , mps_cc_file_sq.config_name      AS config_name
               , mps_cc_in_xms_sq.config_id      AS config_id
               , mps_cc_file_sq.unique_id        AS unique_id
               , mps_cc_file_sq.level1           AS level1
               , mps_cc_file_sq.level2           AS level2
               , mps_cc_file_sq.level3           AS level3
               , mps_cc_file_sq.level4           AS level4
          FROM   mps_cc_file_sq
            INNER JOIN mps_cc_in_xms_sq ON mps_cc_file_sq.unique_id = mps_cc_in_xms_sq.unique_id
          WHERE   mps_cc_file_sq.status = 'ACTIVE'             
        )
      , retiring_mps_cc_sq
      AS
        ( -- Retiring MPS cost codes: MPS cost codes earmarked for soft-deletion (i.e made inactive) in XMS.
          SELECT 'Retiring'                   AS record_type
               , mps_cc_in_xms_sq.config_name AS config_name
               , mps_cc_in_xms_sq.config_id   AS config_id
               , mps_cc_in_xms_sq.unique_id   AS unique_id
               , mps_cc_in_xms_sq.level1      AS level1
               , mps_cc_in_xms_sq.level2      AS level2
               , mps_cc_in_xms_sq.level3      AS level3
               , mps_cc_in_xms_sq.level4      AS level4
          FROM   mps_cc_in_xms_sq
          WHERE  EXISTS ( SELECT NULL
                          FROM   mps_cc_file_sq
                          WHERE  mps_cc_file_sq.unique_id = mps_cc_in_xms_sq.unique_id 
                          AND    mps_cc_file_sq.status = 'INACTIVE' )
        )
      , union_sq -- Consolidated view of New|Existing|Retiring-type MPS cost codes.
      AS
      (
        SELECT *
        FROM   new_mps_cc_sq
        WHERE  COALESCE ( pi_record_type, 'New') = 'New'
        UNION
        SELECT *
        FROM   existing_mps_cc_sq
        WHERE  COALESCE ( pi_record_type, 'Existing') = 'Existing'
        UNION
        SELECT *
        FROM   retiring_mps_cc_sq
        WHERE  COALESCE ( pi_record_type, 'Retiring') = 'Retiring'
      )
      SELECT record_type
           , config_name
           , COALESCE ( config_id
                      , SEQ_CONFIG_ID.NEXTVAL ) AS config_id
           , unique_id
           , level1
           , level2
           , level3
           , level4
      FROM   union_sq;

    RETURN l_cost_code_hierarchy_rc ;
    
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;  
  END get_mps_cost_code_hierarchy_rc ;  

  ------------------------------------------------------------------------------------------------------
  -- PLSQL-2199: procedure to upsert MPS cost code file data into X3 application tables,
  -- CONFIGURATION and COST_CODE.
  ------------------------------------------------------------------------------------------------------
  PROCEDURE upsert_cost_codes ( pi_file_name  IN reed_file_log.file_name%TYPE )
  IS
    -- Local constants...
    lc_process_name              CONSTANT reed_file_log.process_name%TYPE  := 'upsert_cost_codes' ;

    l_no_of_cfg_rows_merged   PLS_INTEGER  := 0; -- Number of CONFIGURATION rows merged in merge statement.
    l_no_of_cfg_rows_inserted PLS_INTEGER  := 0; -- Number of CONFIGURATION rows inserted in merge statement.
    l_no_of_cfg_rows_updated  PLS_INTEGER  := 0; -- Number of CONFIGURATION rows updated in merge statement.
    --
    l_no_of_cc_rows_merged    PLS_INTEGER  := 0; -- Number of COST_CODE rows merged in merge statement.
    l_no_of_cc_rows_inserted  PLS_INTEGER  := 0; -- Number of COST_CODE rows inserted in merge statement.
    l_no_of_cc_rows_updated   PLS_INTEGER  := 0; -- Number of COST_CODE rows updated in merge statement.
    --
    l_no_of_cc_rows_retired   PLS_INTEGER  := 0; -- Number of XMS COST_CODE rows retired as result of MPS cost code load.
    l_no_of_cfg_rows_retired  PLS_INTEGER  := 0; -- Number of XMS CONFIGURATION TABLE rows retired as result of MPS cost code load.

    l_error_index             PLS_INTEGER ; -- for use in bulk DML error logging.

    l_counter                 PLS_INTEGER := 0;

    -- Local Ref Cursors ...
    l_cost_code_hierarchy_rc  cost_code_hierarchy_rct ;

    -- Local arrays...
    l_cost_code_hierarchy_aa  cost_code_hierarchy_aat ;

    -- Bulk exceptions error...
    bulk_errors_exc         EXCEPTION ;
    PRAGMA EXCEPTION_INIT ( bulk_errors_exc, -24381 );

  BEGIN
    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix  => lc_process_name
                  , p_data    => UTL_LMS.FORMAT_MESSAGE ( q'[Started. pi_file_name: '%s'.]'
                                                         , COALESCE ( pi_file_name, '<NULL>' )));

    l_cost_code_hierarchy_rc := get_mps_cost_code_hierarchy_rc;

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => 'Called function get_mps_cost_code_hierarchy_rc.');

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[About to Merge load file '%s' into XMS...]'
                                                       , COALESCE ( (pi_file_name), '<NULL>' )));

    -- Loop through each batched collection of cost codes and merge into the CONFIGURATION and COST_CODE tables..
    LOOP
      FETCH l_cost_code_hierarchy_rc
      BULK  COLLECT
      INTO  l_cost_code_hierarchy_aa
      LIMIT C_BULK_LIMIT ;

      EXIT WHEN l_cost_code_hierarchy_aa.COUNT = 0;

      IF l_counter = 0
      THEN
        reed_dsp.line (' ');
        reed_dsp.line ( p_prefix => lc_process_name
                      , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Merging into table CONFIGURATION...]' ));
      END IF;
      
      ---------------------------------------------------
      -- 1: CONFIGURATION table - MPS Cost codes merge...
      ---------------------------------------------------
      -- Reset the counter used to derive number of inserted/udpated rows for CONFIGURATION merge DML...
      -- (See this Confluence page for a background to this DBMS_APPLICATION_INFO counter technique)
      -- https://reedglobal.atlassian.net/wiki/spaces/PLSQL/pages/2205909473/Hubspot+Business+and+Manager+Data+Upload+to+XMS#DML-MERGE-statement---decomposing-SQL%ROWCOUNT-to-number-of-rows-INSERTED/UPDATED
      DBMS_APPLICATION_INFO.SET_CLIENT_INFO( client_info => 0 );

      -- Merge into configuration table for new/updated MPS cost codes...
      FORALL i IN 1..l_cost_code_hierarchy_aa.COUNT
        SAVE EXCEPTIONS
        MERGE
        INTO  configuration tgt
          USING
          (
            SELECT  l_cost_code_hierarchy_aa(i).record_type  AS record_type
                  , l_cost_code_hierarchy_aa(i).config_id    AS config_id
                  , l_cost_code_hierarchy_aa(i).config_name  AS config_name
                  , l_cost_code_hierarchy_aa(i).level1       AS level1
                  , l_cost_code_hierarchy_aa(i).level2       AS level2
                  , l_cost_code_hierarchy_aa(i).level3       AS level3
                  , l_cost_code_hierarchy_aa(i).level4       AS level4                  
            FROM   dual ) src
          ON  ( src.config_id = tgt.config_id )
        WHEN NOT MATCHED THEN
          INSERT ( config_id
                 , config_type_lookup_val_id
                 , org_id
                 , config_name
                 , created_by_xms_login_id
                 , date_created
                 , updated_by_xms_login_id
                 , last_updated
                 , marked_deleted )
          VALUES ( CASE gla_integration.merge_ins_cnt
                   WHEN 0
                    THEN src.config_id
                   END
                 , C_COST_CODE_LOOKUP_VAL_ID
                 , C_MPS_ORG_ID
                 , src.config_name
                 , C_XMS_LOGIN_ID_ADMIN   -- created_by_xms_login_id
                 , SYSTIMESTAMP           -- date_created
                 , C_XMS_LOGIN_ID_ADMIN   -- updated_by_xms_login_id
                 , SYSTIMESTAMP           -- last_updated
                 , 'N')                   -- marked_deleted
          WHERE  src.record_type = 'New'  -- Restrict to those records identified as new MPS cost code records.
        WHEN MATCHED THEN
          UPDATE
            SET tgt.config_name              = src.config_name
              , tgt.updated_by_xms_login_id  = C_XMS_LOGIN_ID_ADMIN
              , tgt.last_updated             = SYSTIMESTAMP
              , tgt.marked_deleted           = 'N'
            WHERE  1=1
            AND UPPER ( src.config_name ) <> UPPER ( TRIM(tgt.config_name) ) ;
            
      l_no_of_cfg_rows_merged     := l_no_of_cfg_rows_merged   + SQL%ROWCOUNT;
      l_no_of_cfg_rows_inserted   := l_no_of_cfg_rows_inserted + SYS_CONTEXT('USERENV','CLIENT_INFO');
      l_no_of_cfg_rows_updated    := l_no_of_cfg_rows_merged - l_no_of_cfg_rows_inserted;

      IF l_counter = 0
      THEN
        reed_dsp.line (' ');
        reed_dsp.line ( p_prefix => lc_process_name
                      , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Merging into table COST_CODE...]' ));
      END IF;

      ---------------------------------------------------
      -- 2: COST_CODE table - MPS Cost codes merge...
      ---------------------------------------------------
      -- Reset the counter used to derive number of inserted/udpated rows for COST_CODE merge DML...
      -- (See this Confluence page for a background to this DBMS_APPLICATION_INFO counter technique.)
      -- https://reedglobal.atlassian.net/wiki/spaces/PLSQL/pages/2205909473/Hubspot+Business+and+Manager+Data+Upload+to+XMS#DML-MERGE-statement---decomposing-SQL%ROWCOUNT-to-number-of-rows-INSERTED/UPDATED
      DBMS_APPLICATION_INFO.SET_CLIENT_INFO ( client_info => 0 );

      -- Merge into cost code table...
      FORALL i IN 1..l_cost_code_hierarchy_aa.COUNT
        SAVE EXCEPTIONS
        MERGE
        INTO  cost_code tgt
          USING
          (
            SELECT  l_cost_code_hierarchy_aa(i).record_type  AS record_type
                  , l_cost_code_hierarchy_aa(i).config_id    AS config_id
                  , l_cost_code_hierarchy_aa(i).unique_id    AS unique_id
                  , l_cost_code_hierarchy_aa(i).level1       AS level1
                  , l_cost_code_hierarchy_aa(i).level2       AS level2
                  , l_cost_code_hierarchy_aa(i).level3       AS level3
                  , l_cost_code_hierarchy_aa(i).level4       AS level4
            FROM   dual ) src
          ON   ( src.config_id = tgt.cost_cd_config_id )-- src.unique_id = tgt.unique_id )
        WHEN NOT MATCHED THEN
          INSERT ( cost_cd_config_id
                 , unique_id
                 , level1
                 , level2
                 , level3
                 , level4
                 , chargeable -- by default, all MPS cost codes should be chargeable to the client.
                 , marked_deleted )
          VALUES ( CASE gla_integration.merge_ins_cnt
                   WHEN 0
                    THEN src.config_id  -- PK and a FK column in cost_code table.
                   END
                 , src.unique_id
                 , src.level1
                 , src.level2
                 , src.level3
                 , src.level4
                 , 'Y' 
                 , 'N'
                 )
          WHERE  src.record_type = 'New'  -- Restrict to those records identified as new MPS cost code records.
        WHEN MATCHED THEN
          UPDATE
            SET tgt.level1              = src.level1
              , tgt.level2              = src.level2
              , tgt.level3              = src.level3
              , tgt.level4              = src.level4
              , tgt.chargeable          = 'Y' -- by default, all MPS cost codes should be chargeable to the client.
              , tgt.marked_deleted      = 'N' 
              -- No meta-data columns in table cost_code...
              --, tgt.updated_by_xms_login_id  = C_XMS_LOGIN_ID_ADMIN
              --, tgt.last_updated             = SYSTIMESTAMP
            WHERE src.record_type = 'Existing' -- Restrict to those records identified as existing MPS cost code records.
            AND   (
                    COALESCE ( src.level1, 'NULL' ) || '.' ||
                    COALESCE ( src.level2, 'NULL' ) || '.' ||
                    COALESCE ( src.level3, 'NULL' ) || '.' ||
                    COALESCE ( src.level4, 'NULL' ) <> COALESCE ( TRIM(tgt.level1), 'NULL' ) || '.' ||
                                                       COALESCE ( TRIM(tgt.level2), 'NULL' ) || '.' ||
                                                       COALESCE ( TRIM(tgt.level3), 'NULL' ) || '.' ||
                                                       COALESCE ( TRIM(tgt.level4), 'NULL' )
                   OR tgt.chargeable IS NULL -- by default, all MPS cost codes should be chargeable to the client, 
                                             -- so update to 'Y' any cost codes in XMS (matching those in the MPS file) 
                                             -- that haven't previously been loaded as such.
                  ) ; 

      l_no_of_cc_rows_merged     := l_no_of_cc_rows_merged   + SQL%ROWCOUNT;
      l_no_of_cc_rows_inserted   := l_no_of_cc_rows_inserted + SYS_CONTEXT('USERENV','CLIENT_INFO');
      l_no_of_cc_rows_updated    := l_no_of_cc_rows_merged - l_no_of_cc_rows_inserted;

      --------------------------------------------------------------------------------
      -- 3: COST_CODE table - Merge statement to retire any old XMS application cost codes
      -- (i.e soft-delete any XMS cost application codes that have been earmarked for 
      -- deletion in the given MPS cost codes file.)
      --------------------------------------------------------------------------------

      IF l_counter = 0
      THEN
        reed_dsp.line (' ');
        reed_dsp.line ( p_prefix => lc_process_name
                      , p_data   => 'About to soft-delete any retired XMS application cost codes in the COST_CODE table...');
      END IF;

      FORALL i IN 1..l_cost_code_hierarchy_aa.COUNT
        SAVE EXCEPTIONS
        MERGE
        INTO  cost_code tgt
          USING
          (
            SELECT  l_cost_code_hierarchy_aa(i).record_type  AS record_type
                  , l_cost_code_hierarchy_aa(i).config_id    AS config_id
            FROM   dual ) src
          ON   ( src.config_id = tgt.cost_cd_config_id )
        WHEN MATCHED THEN
          UPDATE
            SET tgt.marked_deleted                   = 'Y'
              -- No meta-data columns in table cost_code...
              --, tgt.updated_by_xms_login_id        = C_XMS_LOGIN_ID_ADMIN
              --, tgt.last_updated                   = SYSTIMESTAMP
          WHERE src.record_type     = 'Retiring'  -- Restrict to those XMS records identified as to be retired in the MPS cost code file.
          AND   COALESCE (tgt.marked_deleted, 'N')   = 'N';
          --AND   COALESCE (tgt.marked_default, 'N') = 'N' -- To do: can a default record be retired? Assume so.

      l_no_of_cc_rows_retired := l_no_of_cc_rows_retired + SQL%ROWCOUNT ;

      --------------------------------------------------------------------------------
      -- 4: CONFIGURATION table - Merge statement to retire old XMS application cost codes
      -- (i.e soft-delete any XMS cost application codes earmarked for retiring in the 
      -- given MPS cost codes file.)
      --------------------------------------------------------------------------------

      IF l_counter = 0
      THEN
        reed_dsp.line (' ');
        reed_dsp.line ( p_prefix => lc_process_name
                      , p_data   => 'About to soft-delete retired XMS application cost codes in the CONFIGURATION table...');
      END IF;

      FORALL i IN 1..l_cost_code_hierarchy_aa.COUNT
        SAVE EXCEPTIONS
        MERGE
         INTO  CONFIGURATION tgt
          USING
          (
            SELECT  l_cost_code_hierarchy_aa(i).record_type  AS record_type
                  , l_cost_code_hierarchy_aa(i).config_id    AS config_id
            FROM   dual ) src
          ON   ( src.config_id = tgt.config_id )
        WHEN MATCHED THEN
          UPDATE
            SET tgt.marked_deleted                  = 'Y'
              , tgt.updated_by_xms_login_id         = C_XMS_LOGIN_ID_ADMIN
              , tgt.last_updated                    = SYSTIMESTAMP
          WHERE src.record_type                     = 'Retiring'  -- Restrict to those XMS records identified as to be retired in the MPS cost code file.
          AND   COALESCE (tgt.marked_deleted, 'N')  = 'N';
          --AND   COALESCE (tgt.marked_default, 'N')  = 'N' -- To do: can a default record be retired? Assume so.

      l_no_of_cfg_rows_retired := l_no_of_cfg_rows_retired + SQL%ROWCOUNT ;

      l_counter := l_counter + 1;
    END LOOP;

    CLOSE l_cost_code_hierarchy_rc;

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[%d row(s) merge loaded into table CONFIGURATION. (%d inserted. %d updated).]'
                                                       , TO_CHAR ( l_no_of_cfg_rows_merged )
                                                       , TO_CHAR ( l_no_of_cfg_rows_inserted )
                                                       , TO_CHAR ( l_no_of_cfg_rows_updated )));
    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[%d row(s) merge loaded into table COST_CODE. (%d inserted. %d updated).]'
                                                       , TO_CHAR ( l_no_of_cc_rows_merged )
                                                       , TO_CHAR ( l_no_of_cc_rows_inserted )
                                                       , TO_CHAR ( l_no_of_cc_rows_updated )));

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[%d XMS row(s) retired in table COST_CODE.]'
                                                       , TO_CHAR ( l_no_of_cc_rows_retired )));

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[%d XMS row(s) retired in table CONFIGURATION.]'
                                                       , TO_CHAR ( l_no_of_cfg_rows_retired )));

    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Ended.]'));

  EXCEPTION
    WHEN bulk_errors_exc THEN
      IF l_cost_code_hierarchy_rc%ISOPEN
      THEN
        CLOSE l_cost_code_hierarchy_rc ;
      END IF ;

      FOR i IN 1..SQL%BULK_EXCEPTIONS.COUNT
      LOOP
        l_error_index := SQL%BULK_EXCEPTIONS (i).ERROR_INDEX ;

        reed_dsp.line ( p_prefix      => lc_process_name
                      , p_data        => UTL_LMS.FORMAT_MESSAGE ( q'[Error #%d loading file '%s' for merge update iteration #%d. config_name: %s config_id: %s unique_id: %s - Error message: %s.]'
                                                                , TO_CHAR (i)
                                                                , COALESCE ( (pi_file_name), '<NULL>' )
                                                                , l_error_index
                                                                , l_cost_code_hierarchy_aa (l_error_index).config_name
                                                                , TO_CHAR (l_cost_code_hierarchy_aa (l_error_index).config_id)
                                                                , l_cost_code_hierarchy_aa (l_error_index).unique_id
                                                                , SQLERRM ( -1 * SQL%BULK_EXCEPTIONS (i).ERROR_CODE ))
                      , p_trace_level => reed_dsp.trace_level_error );
      END LOOP;
      ROLLBACK;
      RAISE ;

    WHEN OTHERS THEN
      IF l_cost_code_hierarchy_rc%ISOPEN
      THEN
        CLOSE l_cost_code_hierarchy_rc ;
      END IF ;

      reed_dsp.line( p_prefix      => lc_process_name
                   , p_data        => UTL_LMS.FORMAT_MESSAGE ( q'['Error in %s.%s loading file '%s' - OTHERS - %s.]'
                                                             , C_PACKAGE_NAME
                                                             , lc_process_name
                                                             , COALESCE ( (pi_file_name), '<NULL>' )
                                                             , SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE )
                   , p_trace_level => reed_dsp.trace_level_error );
      ROLLBACK;
      RAISE;
  END upsert_cost_codes;  
  
  ------------------------------------------------------------------------------------------------------------------------  
  -- Validate the MPS cost code file for any invalid data records. 
  -- Note: this is just a basic validation for the initial live release (Summer 2022) and may require more elegant validation
  -- development depending on any MPS cost code file data quality issues discovered post-intitial release.
  ------------------------------------------------------------------------------------------------------------------------  
  PROCEDURE validate_mps_cost_code_file ( pi_file_name IN user_external_locations.location%TYPE  )
  IS
    lc_process_name    CONSTANT reed_file_log.process_name%TYPE  := 'validate_mps_cost_code_file' ;        
    l_number_of_issues PLS_INTEGER := 0; -- Number of data validation issues discovered in the MPS cost code file.  
  BEGIN
    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Started. pi_file_name: '%s']'
                                                       , pi_file_name ));  
    SELECT COUNT (*)
    INTO   l_number_of_issues
    FROM   mps_cost_code_ext
    WHERE  UPPER (COALESCE (status, 'XxX')) NOT IN ('INACTIVE', 'ACTIVE'); 
    
    IF l_number_of_issues > 0
    THEN
       -- Log error (only if logging is enabled)...
      reed_dsp.line (' ');  -- Newline
      reed_dsp.line ( p_prefix => lc_process_name
                    , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[MPS cost code file '%s' contains %d record(s) with invalid status codes.]'
                                                         , pi_file_name
                                                         , l_number_of_issues ));
      RAISE_APPLICATION_ERROR ( num => -20200
                              , msg => UTL_LMS.FORMAT_MESSAGE ( q'[Error: MPS cost code file '%s' contains %d record(s) with invalid status codes.]'
                                                              , pi_file_name
                                                              , l_number_of_issues ));
    END IF;

    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => 'Ended.');

  EXCEPTION
    WHEN OTHERS THEN
     RAISE ;    
  END validate_mps_cost_code_file ;
  
  ------------------------------------------------------------------------------------------------------
  -- *** Top-level procedure for loading MPS cost codes. *** 
  -- PLSQL-2199: Procedure to load MPS (Met Police Service) cost codes from the MPS cost code file.
  -- This procedure is either called by the generic MPS file watcher file load procedure, load_file, 
  -- or can be called directly for manually loading an MP cost code file. 
  -- 
  -- pi_debug: debug boolean defaulted to FALSE. If set, a load log file will be produced in the
  -- UTL_SFTP_MPS_IN_ARC directory to help with post-release analysis/debugging. 
  -- This can only be set for manual MPS file loads, rather than automated (file watcher) loads, so the
  -- normal operation will be that no log file is produced.
  ------------------------------------------------------------------------------------------------------
  PROCEDURE load_mps_cost_codes ( pi_file_name IN user_external_locations.location%TYPE 
                                , pi_debug     IN BOOLEAN DEFAULT FALSE ) 
  IS
    -- Local constants...
    lc_process_name           CONSTANT reed_file_log.process_name%TYPE  := 'load_mps_cost_codes' ;
    lc_file_name              CONSTANT reed_file_log.file_name%TYPE     := COALESCE ( pi_file_name, '<UNKNOWN>' );
    --lc_err_report_file_name   CONSTANT reed_file_log.file_name%TYPE     := REPLACE ( lc_file_name, '.csv', '_data_validation_report.csv' );

    -- Local variables...
    l_reed_file_log_id       reed_file_log.reed_file_log_id%TYPE ;
    l_result_obj             SYS.SCHEDULER_FILEWATCHER_RESULT
                         :=  SYS.SCHEDULER_FILEWATCHER_RESULT  ( NULL          -- destination
                                                               , C_DIRECTORY_MPS_IN
                                                               , lc_file_name  -- Actual file name
                                                               , 0             -- file size
                                                               , SYSTIMESTAMP  -- file t/stamp
                                                               , 0             -- ts_ms_from_epoch
                                                               , NULL );       -- matching_requests
  BEGIN
    -- Re-populate the session variables so that g_mps_cc_load_trace_file_name is refreshed with the latest system date...
    populate_session_variables;

    -- Switch on trace log file logging only if debug switched on...
    IF pi_debug
    THEN
      switch_on_trace ( pi_trace_file_name    => g_mps_cc_load_trace_file_name
                      , pi_new_trace_file_dir => C_DIRECTORY_MPS_IN_ARC );
    ELSE 
      -- Ensure trace log file logging switched off...
      switch_on_trace ( pi_switch_on => FALSE );    
    END IF;
    
    --reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Started. MPS Org ID: '%d']'
                                                        , TO_CHAR (mps_integration.mps_org_id)));

    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Running in '%s' environment.]'
                                                         , C_DB_ENVIRONMENT ));

    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[MPS cost code file name: '%s'.]' -- MPS data validation report file name: %s.]'
                                                       , lc_file_name ));
                                                       --, lc_err_report_file_name));

    IF NOT file_exists ( pi_location => C_DIRECTORY_MPS_IN
                       , pi_filename => pi_file_name )
    THEN
      RAISE_APPLICATION_ERROR ( num => -20100
                              , msg => UTL_LMS.FORMAT_MESSAGE ( q'[File '%s' does not exist in directory '%s']'
                                                              , pi_file_name
                                                              , C_DIRECTORY_MPS_IN ));
    ELSE
      reed_dsp.line (' ');  -- Newline
      reed_dsp.line ( p_prefix => lc_process_name
                    , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[About to change table MPS_COST_CODE_EXT location to '%s'...]'
                                                         , lc_file_name));
                                                         
      -- Point the MPS cost code external table to the specified MPS cost code file...
      EXECUTE IMMEDIATE (UTL_LMS.FORMAT_MESSAGE ( q'[ALTER TABLE mps_cost_code_ext LOCATION ('%s')]'
                                                , lc_file_name ));
      reed_dsp.line (' ');  -- Newline
      reed_dsp.line ( p_prefix => lc_process_name
                    , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[Table location changed.]'));
    END IF;

    -- Log file load into REED_FILE_LOG and get the reed file log number for this load...
    log_new_file ( pi_result             => l_result_obj
                 , pi_process_name       => lc_process_name
                 , po_reed_file_number   => l_reed_file_log_id );

    IF l_reed_file_log_id IS NULL
    THEN
      -- Would need to explore why this is NULL so raise an error...
      ROLLBACK;
      RAISE_APPLICATION_ERROR  ( num => -20500
                               , msg => UTL_LMS.FORMAT_MESSAGE ( q'[Error raised in %s.%s loading MPS cost code file '%s'. Unable to assign a Reed File Log ID for the file load.]'
                                                               , C_PACKAGE_NAME
                                                               , lc_process_name
                                                               , lc_file_name
                                                               ));

    END IF;

    -- Validate the MPS cost code file; basic check and will raise an exception if status fields are not agreed values.
    -- May require more elegant solution post-initial release (Summer 2022) depending on MPS cost code file data quality...
    validate_mps_cost_code_file ( pi_file_name => pi_file_name );
        
    -- Upsert the MPS Cost Code file data into X3 application tables, CONFIGURATION and COST_CODE...
    upsert_cost_codes  ( pi_file_name => lc_file_name ) ;

    COMMIT;
                                    
    -- Note: SOLR indexing? Not required for cost codes.
    
    reed_dsp.line (' ');  -- Newline
    reed_dsp.line ( p_prefix => lc_process_name
                  , p_data   => UTL_LMS.FORMAT_MESSAGE ( q'[About to move file '%s' from inbound directory '%s' to archive directory '%s'...]'
                                                       , lc_file_name
                                                       , C_DIRECTORY_MPS_IN
                                                       , C_DIRECTORY_MPS_IN_ARC
                                                       ));
    -- Archive successfully loaded file...    
    archive_mps_inbound_file ( pi_file_name => lc_file_name ) ;    
    
    reed_dsp.line (' ');
    reed_dsp.line ( p_prefix  => lc_process_name
                  , p_data    => UTL_LMS.FORMAT_MESSAGE ( q'[Ended MPS cost code load for file name: %s]'
                                                        , lc_file_name ));
                                                        
    -- Switch off trace log file logging if run in debug mode...
    IF pi_debug
    THEN                                                        
      -- Switch off trace log file logging...
      switch_on_trace ( pi_switch_on => FALSE );
    END IF;

  EXCEPTION

    WHEN OTHERS THEN
      ROLLBACK;
      
      IF pi_debug
      THEN
        -- Log actual underlying error to the trace log file, and re-raise a 'friendly', catch-all error message...
        reed_dsp.line (' ');  -- Newline        
        reed_dsp.line( p_prefix      => lc_process_name
                     , p_data        => UTL_LMS.FORMAT_MESSAGE ( q'['Error in %s.%s loading Reed file log ID '%s' - OTHERS - %s.]'
                                                               , C_PACKAGE_NAME
                                                               , lc_process_name
                                                               , COALESCE (TO_CHAR ( l_reed_file_log_id ), '<NULL>')
                                                               , SQLERRM || CHR(10) || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE )
                     , p_trace_level => reed_dsp.trace_level_error );
  
        -- User-defined catch-all error message...
        RAISE_APPLICATION_ERROR  ( num => -20425
                                 , msg => UTL_LMS.FORMAT_MESSAGE ( q'[Error raised in %s.%s loading MPS cost code file '%s'. See trace file '%s' in '%s' for more information.]'
                                                                 , C_PACKAGE_NAME
                                                                 , lc_process_name
                                                                 , lc_file_name
                                                                 , COALESCE (g_mps_cc_load_trace_file_name, '<NULL>')
                                                                 , C_DIRECTORY_MPS_IN_ARC
                                                                 ));  
      ELSE
        RAISE;      
      END IF;
  END load_mps_cost_codes;
  
  ----------------------------------------------------------------------------------------------------
  -- Procedure to process a data file.
  -- Calls appropriate procedure for the file type (based on file name).
  ----------------------------------------------------------------------------------------------------
  PROCEDURE load_file (
    pi_file_name                   IN     all_external_locations.location%TYPE
  )
  IS
  BEGIN
    dbms_output.put_line( 'Procedure: mps_integration.load_file. pi_file_name: ' || pi_file_name ) ;

    IF pi_file_name LIKE 'XMS_POSITIONS_%.csv' THEN
      load_positions( pi_file_name => pi_file_name ) ;
    ELSIF pi_file_name LIKE 'XMS_AC_%.csv' THEN
      load_activity_codes( pi_file_name => pi_file_name ) ;
    ELSIF pi_file_name LIKE 'XMS_AGENCY_%.csv' THEN
      load_mps_emp_bkg( pi_file_name => pi_file_name ) ;
    ELSIF pi_file_name LIKE 'XMS_CC_%.csv' THEN
      load_mps_cost_codes( pi_file_name => pi_file_name ) ;
    ELSIF pi_file_name LIKE '%.bad' THEN
      NULL ; -- BADFILE containing invalid data
    ELSE
      RAISE_APPLICATION_ERROR( -20001, 'File type cannot be identified from file name "' || pi_file_name || '"' ) ;
    END IF ;
  END load_file ;


  ----------------------------------------------------------------------------------------------------
  -- Procedure to process a data file.
  -- Will be called by a file watcher and therefore has a SCHEDULER_FILEWATCHER_RESULT parameter.
  ----------------------------------------------------------------------------------------------------
  PROCEDURE load_file (
    pi_filewatcher_result          IN     SYS.SCHEDULER_FILEWATCHER_RESULT
  )
  IS
  BEGIN
    load_file( pi_file_name => pi_filewatcher_result.actual_file_name ) ;
  END load_file ;
  
  FUNCTION next_mpsrminimasterlogid RETURN NUMBER IS
  BEGIN
    RETURN seq_mps_hr_minimaster_log_file_id.nextval;
  END next_mpsrminimasterlogid ;
  
  ----------------------------------------------------------------------------------------------------
  -- Procedure to generate the MPS HR Minimaster Extract Files. Files of the form
  -- XMS_CWC_YYYYMMDDHH24MISS.xlsx for New Hires
  -- XMS_CWLA_YYYYMMDDHH24MISS.xlsx for Leavers and Amendments
  
  -- It uses global temporary table mps_hr_minimaster_gtt as the driving table to create extracted rows.
  -- The GTT is populated by PopulateExtractTrackingTable beforehand.
  ----------------------------------------------------------------------------------------------------
  
  PROCEDURE generate_hr_extract_files 
  IS 
    v_new_hire_rows_processed   NUMBER :=0;
    v_amendment_rows_processed  NUMBER :=0;
    
    lc_directory all_directories.directory_name%TYPE := C_DIRECTORY_MPS_OUT;
    
    C_CWLC_BASE_NAME  CONSTANT VARCHAR2(30 CHAR) := 'MPS_CWC_';
    C_CWLA_BASE_NAME  CONSTANT VARCHAR2(30 CHAR) := 'MPS_CWLA_';
    C_EXTENSION               CONSTANT VARCHAR2(10 CHAR)   := '.xlsx';
    
    lc_date_format                  VARCHAR2(100)      := 'YYYYMMDDHH24MISS';
    lc_cwlc_filename                VARCHAR2(100 CHAR);
    lc_cwla_filename                VARCHAR2(100 CHAR);

    lc_timestamp                      TIMESTAMP WITH TIME ZONE := systimestamp  AT TIME ZONE 'Europe/London';
    
    cur_cwc_rows                    SYS_REFCURSOR;
    cur_cwla_rows                   SYS_REFCURSOR;
    
    l_sheet_number                  PLS_INTEGER := 0 ;

    v_now DATE := SYSDATE;         
    
    v_file_id                        PLS_INTEGER ;

  BEGIN

    PopulateExtractTrackingTable(p_rows_to_process_new_hires => v_new_hire_rows_processed, p_rows_to_process_amendment => v_amendment_rows_processed);
    dbms_output.put_line('v_new_hire_rows_processed = '|| v_new_hire_rows_processed ||' v_amendment_rows_processed = '|| v_amendment_rows_processed);
    
    IF v_new_hire_rows_processed > 0 THEN
    
      lc_cwlc_filename := C_CWLC_BASE_NAME || to_char(v_now, lc_date_format)|| C_EXTENSION;      
      
      reed_xlsx.clear_workbook;

      l_sheet_number := l_sheet_number + 1 ;
      reed_xlsx.new_sheet(lc_cwlc_filename) ;
      
      -- Format rows (including header row)
      FOR i IN 1 .. v_new_hire_rows_processed + 1 -- Include header row, so one more than the total number of validation issues.
      LOOP
        reed_xlsx.set_row (
          p_row         => i
        , p_fontId      => reed_xlsx.get_font( p_name      => 'Calibri'
                                             , p_fontsize  => 10
                                             , p_bold      => CASE WHEN i = 1 THEN TRUE END
                                             , p_rgb       => CASE WHEN i = 1 THEN 'FF1F497D' END ) -- p_rgb is an RGBA value. The 2 most significant digits are the alpha value that specifies transparency/opacity as a percentage (00 = fully transparent, FF = fully opaque).
        , p_alignment   => reed_xlsx.get_alignment( p_vertical => 'center' )
        ) ;
      END LOOP ;      
      
      
      OPEN cur_cwc_rows FOR
      SELECT    
      FIRST_NAME AS "S02_First_Name",
      LAST_NAME AS "S02_Last_Name",
      TITLE AS "S02_Title",
      MIDDLE_NAME AS "S02_Middle_Name",
      GENDER AS "S02_Gender",
      PERSON_TYPE AS "S02_Person_Type",
      NI_NUMBER AS "S02_NI_Number",
      TEMPORARY_GRADE AS "S02_Temporary_Grade_(NS)",
      to_char(START_DATE, C_EXTRACT_DATE_FORMAT) AS "S03_Start_Date_(NS)",
      SUPERVISOR_ID AS "S03_Supervisor_ Assignment_Number (NS)",
      MPS_POSITION AS "S03_Position_(NS)",
      BOOKING_ID AS "XMS_Booking_ID"    
      FROM mps_hr_minimaster_gtt
      WHERE request_type = C_REQUEST_TYPE_NEW_HIRE
      ORDER BY booking_id;

      reed_xlsx.query2sheet(cur => cur_cwc_rows, p_sheet => l_sheet_number, p_usexf => TRUE) ;

      -- Save spreadsheet
      reed_xlsx.save( p_directory => lc_directory, p_filename => lc_cwlc_filename ) ;                
    
      -- Write same records to the logging table
      v_file_id  := seq_mps_hr_minimaster_log_file_id.NEXTVAL;
      
      INSERT INTO mps_hr_minimaster_log(
      MPS_HR_MINIMASTER_LOG_ID,
      FILE_ID,
      FILE_NAME,
      REQUEST_TYPE,
      PROCESS_DATE,
      FIRST_NAME,
      LAST_NAME,
      TITLE,
      MIDDLE_NAME,
      GENDER,
      PERSON_TYPE,
      NI_NUMBER,
      TEMPORARY_GRADE,
      START_DATE,
      SUPERVISOR_ID,
      MPS_POSITION,
      BOOKING_ID
      )
      SELECT 
      next_mpsrminimasterlogid,
      v_file_id,
      lc_cwlc_filename,
      REQUEST_TYPE,
      v_now,
      FIRST_NAME,
      LAST_NAME,
      TITLE,
      MIDDLE_NAME,
      GENDER,
      PERSON_TYPE,
      NI_NUMBER,
      TEMPORARY_GRADE,
      START_DATE,
      SUPERVISOR_ID,
      MPS_POSITION,
      BOOKING_ID      
      FROM mps_hr_minimaster_gtt
      WHERE request_type = C_REQUEST_TYPE_NEW_HIRE
      ORDER BY booking_id;      
      
      MERGE INTO mps_hr_minimaster_tracker 
      USING  mps_hr_minimaster_gtt 
      ON (mps_hr_minimaster_tracker.booking_id = mps_hr_minimaster_gtt.booking_id 
          AND mps_hr_minimaster_gtt.request_type = C_REQUEST_TYPE_NEW_HIRE)
      WHEN MATCHED THEN UPDATE SET      
        process_status = C_HRMINIMASTER_MPS_PROCESSED,
        process_date   =  lc_timestamp;
      
      COMMIT ;
      
    END IF;
    

    l_sheet_number := 0;
    
    IF v_amendment_rows_processed > 0 THEN
      
      lc_cwla_filename := C_CWLA_BASE_NAME || to_char(v_now, lc_date_format)|| C_EXTENSION;
      
      reed_xlsx.clear_workbook;

      l_sheet_number := l_sheet_number + 1 ;
      reed_xlsx.new_sheet(lc_cwla_filename) ;
      
      -- Format rows (including header row)
      FOR i IN 1 .. v_amendment_rows_processed + 1 -- Include header row, so one more than the total number of validation issues.
      LOOP
        reed_xlsx.set_row (
          p_row         => i
        , p_fontId      => reed_xlsx.get_font( p_name      => 'Calibri'
                                             , p_fontsize  => 10
                                             , p_bold      => CASE WHEN i = 1 THEN TRUE END
                                             , p_rgb       => CASE WHEN i = 1 THEN 'FF1F497D' END ) -- p_rgb is an RGBA value. The 2 most significant digits are the alpha value that specifies transparency/opacity as a percentage (00 = fully transparent, FF = fully opaque).
        , p_alignment   => reed_xlsx.get_alignment( p_vertical => 'center' )
        ) ;
      END LOOP ;      
          
      OPEN cur_cwla_rows FOR
      WITH combine_qry AS (
      SELECT 
      BOOKING_ID,
      REQUEST_TYPE, 
      NULL AS MPS_WARRANT_NUMBER_A,                    -- MPS Warranr Number (Amendment)
      NULL AS NI_NUMBER_A,                    -- NI Number (Amendment)
      NULL AS TEMPORARY_GRADE_A,              -- Temp Grade (Amendment) 
      NULL AS  EFFECTVE_DATE_OF_CHANGE_A,     --  effective date of change (Amendment)
      NULL AS SUPERVISOR_ID_A,                   -- Supervisor ID (Amendment)
      MPS_WARRANT_NUMBER AS MPS_WARRANT_NUMBER_L ,     -- MPS Warranr Number (Leaver) 
      to_char(LEAVING_DATE,C_EXTRACT_DATE_FORMAT) AS LEAVING_DATE_L          -- Leaving Date (Leaver) 
      FROM mps_hr_minimaster_gtt
      WHERE request_type = C_REQUEST_TYPE_LEAVER
      UNION ALL
      SELECT
      BOOKING_ID,
      REQUEST_TYPE,                 
      MPS_WARRANT_NUMBER AS MPS_WARRANT_NUMBER_A,        -- MPS Warranr Number (Amendment)
      NI_NUMBER AS NI_NUMBER_A,                          -- NI Number (Amendment)
      TEMPORARY_GRADE AS TEMPORARY_GRADE_A,     -- Temp Grade (Amendment) 
      to_char(EFFECTVE_DATE_OF_CHANGE, C_EXTRACT_DATE_FORMAT)  AS EFFECTVE_DATE_OF_CHANGE_A,       --  effective date of change (Amendment)  
      SUPERVISOR_ID AS SUPERVISOR_ID_A,            -- Supervisor ID (Amendment)
      NULL AS MPS_WARRANT_NUMBER_L,      
      NULL AS LEAVING_DATE_L       -- MPS Warranr Number (Leaver) 
      FROM mps_hr_minimaster_gtt
      WHERE request_type = C_REQUEST_TYPE_AMENDMENT )
      SELECT 
      REQUEST_TYPE AS "Request_Type", 
      MPS_WARRANT_NUMBER_A AS "S02_Person UN_(A)",
      NI_NUMBER_A AS "S02_NI Number_(A)",
      TEMPORARY_GRADE_A AS "S02_Temporary Grade_(A)",
      EFFECTVE_DATE_OF_CHANGE_A AS "S03_Effective_Date of_Change_(A)",
      SUPERVISOR_ID_A AS "S03_Supervisor_(A)",
      MPS_WARRANT_NUMBER_L AS "S02_Leaver_UN_(L)",  
      LEAVING_DATE_L AS "S03_Leaving Date_(L)"      
      FROM combine_qry     
      ORDER BY CASE WHEN request_type = C_REQUEST_TYPE_LEAVER THEN 1 ELSE 2 END, booking_id;
      
      reed_xlsx.query2sheet(cur => cur_cwla_rows, p_sheet => l_sheet_number, p_usexf => TRUE) ;

      -- Save spreadsheet
      reed_xlsx.save( p_directory => lc_directory, p_filename => lc_cwla_filename ) ;                
    
      -- Write same records to the logging table
      v_file_id  := seq_mps_hr_minimaster_log_file_id.NEXTVAL;
      
      INSERT INTO mps_hr_minimaster_log(
      MPS_HR_MINIMASTER_LOG_ID,
      FILE_ID,
      FILE_NAME,
      REQUEST_TYPE,
      BOOKING_ID,
      PROCESS_DATE,
      MPS_WARRANT_NUMBER,
      LEAVING_DATE
      )      
      SELECT
      next_mpsrminimasterlogid,
      v_file_id,
      lc_cwla_filename,
      REQUEST_TYPE,
      booking_id,
      v_now,      
      MPS_WARRANT_NUMBER,
      LEAVING_DATE
      FROM mps_hr_minimaster_gtt
      WHERE request_type =   C_REQUEST_TYPE_LEAVER;
      
      INSERT INTO mps_hr_minimaster_log(
      MPS_HR_MINIMASTER_LOG_ID,
      FILE_ID,
      FILE_NAME,
      REQUEST_TYPE,
      BOOKING_ID,
      PROCESS_DATE,
      MPS_WARRANT_NUMBER,
      NI_NUMBER,
      TEMPORARY_GRADE,
      EFFECTVE_DATE_OF_CHANGE,
      SUPERVISOR_ID)
      SELECT 
      next_mpsrminimasterlogid,
      v_file_id,
      lc_cwla_filename,
      REQUEST_TYPE,
      booking_id,
      v_now,      
      MPS_WARRANT_NUMBER,
      NI_NUMBER,
      TEMPORARY_GRADE,
      EFFECTVE_DATE_OF_CHANGE,
      SUPERVISOR_ID      
      FROM mps_hr_minimaster_gtt
      WHERE request_type =   C_REQUEST_TYPE_AMENDMENT;
      
      MERGE INTO mps_hr_minimaster_tracker 
      USING  mps_hr_minimaster_gtt 
      ON (mps_hr_minimaster_tracker.booking_id = mps_hr_minimaster_gtt.booking_id 
          AND least(mps_hr_minimaster_gtt.request_type) IN (C_REQUEST_TYPE_LEAVER, C_REQUEST_TYPE_AMENDMENT))
      WHEN MATCHED THEN UPDATE SET
          supervisor_id  = CASE WHEN mps_hr_minimaster_tracker.request_type = C_REQUEST_TYPE_AMENDMENT THEN mps_hr_minimaster_gtt.supervisor_id ELSE mps_hr_minimaster_tracker.supervisor_id END,      
          process_status = C_HRMINIMASTER_MPS_PROCESSED,
          request_type   = mps_hr_minimaster_gtt.request_type,
          process_date   =  lc_timestamp;
       
      
      COMMIT;
      
    END IF;
        
  END generate_hr_extract_files;

  
  PROCEDURE PopulateExtractTrackingTable(
    p_rows_to_process_new_hires OUT NUMBER,
    p_rows_to_process_amendment OUT NUMBER
  ) 
  IS
    v_now DATE := SYSDATE;
    v_rows_request_type_existing NUMBER;  
      
  BEGIN

    -- purge global temporary table just in case procedure is being called from SQL developer after a previous call to the procedure
    DELETE FROM mps_hr_minimaster_gtt;
    
    INSERT INTO mps_hr_minimaster_tracker(booking_id, item_id, candidate_id, process_Status, request_type)
    SELECT booking.booking_id, booking.item_id, 
    booking.cand_id, 
    C_HRMINIMASTER_MPS_PROCESSING AS process_status, 
    C_REQUEST_TYPE_NEW_HIRE  AS request_type
    FROM orders 
    INNER JOIN item on item.order_id = orders.order_id
    INNER JOIN booking on booking.item_id = item.item_id
    INNER JOIN extension on booking.booking_id = extension.booking_id and extension.marked_deleted = 'N' and extension.active = 'Y'    
    LEFT JOIN mps_hr_minimaster_tracker ON mps_hr_minimaster_tracker.booking_id = booking.booking_id     
    WHERE orders.org_id = C_MPS_ORG_ID     
    AND orders.marked_deleted = 'N'
    AND booking_status1_lookup_val_id  = 576  --(Open Bookings Only)    
    AND booking.booking_status2_lookup_val_id NOT IN (577, 579)  -- ignore Requested and Rejected. Include all others.
    AND booking.client_identifier IS NULL  -- MPS Warrant Number (Employee number) will not be set for New Hires yet. A candidate will be given a new Employee Number PER booking. E.g. if a Candidate moves roles, a Leaver action is triggered for the existing booking and a New Hire for a new booking with a different Employee Number
    AND mps_hr_minimaster_tracker.booking_id IS NULL -- Not yet registered within tracker table   
    ; 

    p_rows_to_process_new_hires := SQL%ROWCOUNT;
    
    COMMIT;
    
    
    -- PLSQL-2299 - Add rows to tracker where warrant number populated. These will not feature in New Hires CWC file, but will be subject to future Amendment and Leaver action 
    INSERT INTO mps_hr_minimaster_tracker(booking_id, item_id, candidate_id, supervisor_id, process_Status, process_date, request_type)
    SELECT booking.booking_id, booking.item_id, 
    booking.cand_id, 
    trim(client.company_name)  AS supervisor_id,    
    C_HRMINIMASTER_MPS_PROCESSED AS process_status, 
    v_now - interval '1' second,
    C_REQUEST_TYPE_EXISTING AS request_type
    FROM orders 
    INNER JOIN item on item.order_id = orders.order_id
    INNER JOIN booking on booking.item_id = item.item_id
    INNER JOIN extension on booking.booking_id = extension.booking_id and extension.marked_deleted = 'N' and extension.active = 'Y'    
    LEFT JOIN mps_hr_minimaster_tracker ON mps_hr_minimaster_tracker.booking_id = booking.booking_id     
      LEFT JOIN role additional_authoriser_role ON booking.additional_auth_tier1_role_id  = additional_authoriser_role.role_id AND additional_authoriser_role.marked_deleted  = 'N'       
      LEFT JOIN person ON person.person_id = additional_authoriser_role.person_id AND person.marked_deleted = 'N'
      LEFT JOIN client ON additional_authoriser_role.person_id = client.person_id and additional_authoriser_role.org_id = client.org_id and client.marked_Deleted = 'N'                        
    WHERE orders.org_id = C_MPS_ORG_ID     
    AND orders.marked_deleted = 'N'
    AND booking_status1_lookup_val_id  = 576  --(Open Bookings Only)    
    AND booking.booking_status2_lookup_val_id NOT IN (577, 579)  -- ignore Requested and Rejected. Include all others.
    AND booking.client_identifier IS NOT NULL     
    AND mps_hr_minimaster_tracker.booking_id IS NULL -- Not yet registered within tracker table   
    ; 
    
    v_rows_request_type_existing := SQL%ROWCOUNT;
    
    dbms_output.put_line('v_rows_request_type_existing = '|| v_rows_request_type_existing);
    
    COMMIT ;

    IF  p_rows_to_process_new_hires > 0 THEN
   
      INSERT INTO mps_hr_minimaster_gtt (
      booking_id, 
      item_id, 
      start_date, 
      candidate_id, 
      first_name, 
      last_name, 
      middle_name, 
      title, 
      gender, 
      person_type, 
      ni_number,   
      temporary_grade,   
      supervisor_id, 
      mps_position,
      request_type
      )
      WITH xms_mps_title_map as (
      SELECT title_lookup_value_id, xms_title, mps_title, gender
      FROM xms_parameters
      CROSS JOIN JSON_TABLE(
      value ,
      '$[*]'
      COLUMNS ( 
           Title_Lookup_value_id NUMBER path  '$.Title_Lookup_value_id' ERROR ON ERROR,
            XMS_Title varchar2(30 CHAR) path '$.XMS_Title' ERROR ON ERROR,
            MPS_Title VARCHAR2(30 CHAR) path '$.MPS_Title' ERROR ON ERROR,
            Gender VARCHAR2(2 CHAR) path '$.Gender' ERROR ON ERROR                                    
          )
      )
      WHERE name = C_TITLE_GENDER_MAP
      )        
      SELECT
      mps_hr_minimaster_tracker.booking_id,
      mps_hr_minimaster_tracker.item_id,
      cast(ob.effective_Date AS DATE) as start_date,
      mps_hr_minimaster_tracker.candidate_id,
      candidate.first_name,      
      candidate.last_name,
      candidate.middle_name, 
      xms_mps_title_map.mps_title,
      COALESCE(CASE upper(gender.lookup_val_detail)
      when 'MALE' THEN 'Male'
      when 'FEMALE' THEN 'Female'
      ELSE
        CASE xms_mps_title_map.gender WHEN '1' THEN 'Male' WHEN '2' THEN 'Female' WHEN '-1' THEN 'Unknown Gender'  END
      END,'Unknown Gender') as Gender,
      C_PERSON_TYPE As person_type,
      candidate.ni_number,
      C_TEMPORARY_GRADE As temporary_grade,
      trim(client.company_name)  AS supervisor_id,       
      trim(mps_position.client_code_name) AS mps_position,
      mps_hr_minimaster_tracker.request_type
      FROM mps_hr_minimaster_tracker
      INNER JOIN candidate ON mps_hr_minimaster_tracker.candidate_id = candidate.cand_id
      INNER JOIN booking ON mps_hr_minimaster_tracker.booking_id = booking.booking_id
      INNER JOIN extension on booking.booking_id = extension.booking_id and extension.marked_deleted = 'N' and extension.active = 'Y'    
      INNER JOIN Extension ob
              ON ob.booking_id = booking.booking_id
              AND ob.marked_deleted = 'N'
              AND ob.original_booking = 'Y'
              AND (
                ob.extension_status_lookup_val_id = 4671
                OR ob.active = 'Y'
                OR ( ob.extension_status_lookup_val_id = 5269) AND ( coalesce(ob.hidden_amendment,'Y') = 'N' )
              )           
      LEFT JOIN lookup_value title_lookup ON title_lookup.lookup_val_id = candidate.title_lookup_val_id
      LEFT JOIN Diversity on candidate.cand_id = Diversity.cand_id and diversity.marked_Deleted = 'N'
      LEFT JOIN lookup_value gender ON Diversity.gender_lookup_val_id = gender.lookup_val_id          
      LEFT JOIN xms_mps_title_map ON xms_mps_title_map.title_lookup_value_id = COALESCE(candidate.title_lookup_val_id, 6018) -- IF title undefine, default to Mx
      LEFT JOIN client_code mps_position ON mps_position.client_code_id = booking.client_code_1_id AND mps_position.client_code_type_lookup_val_id = 6029 AND mps_position.org_id = C_MPS_ORG_ID 
      LEFT JOIN role additional_authoriser_role ON booking.additional_auth_tier1_role_id  = additional_authoriser_role.role_id AND additional_authoriser_role.marked_deleted  = 'N'       
      LEFT JOIN person ON person.person_id = additional_authoriser_role.person_id AND person.marked_deleted = 'N'
      LEFT JOIN client ON additional_authoriser_role.person_id = client.person_id and additional_authoriser_role.org_id = client.org_id and client.marked_Deleted = 'N'                              
      WHERE mps_hr_minimaster_tracker.process_status = C_HRMINIMASTER_MPS_PROCESSING
        AND least(mps_hr_minimaster_tracker.request_type)     = C_REQUEST_TYPE_NEW_HIRE;
      
      -- plsql-2305
      MERGE INTO mps_hr_minimaster_tracker 
      USING  mps_hr_minimaster_gtt 
      ON (mps_hr_minimaster_tracker.booking_id = mps_hr_minimaster_gtt.booking_id AND 
          mps_hr_minimaster_gtt.request_type = C_REQUEST_TYPE_NEW_HIRE  AND
          mps_hr_minimaster_tracker.process_Status = C_HRMINIMASTER_MPS_PROCESSING
          )
      WHEN MATCHED THEN UPDATE SET    
        supervisor_id  =  mps_hr_minimaster_gtt.supervisor_id;
        
      COMMIT;
   
    END IF;
    
    -- Amendments Part 1: Leavers first
    INSERT INTO mps_hr_minimaster_gtt (
    booking_id, 
    item_id, 
    candidate_id,
    mps_warrant_number,
    leaving_date,
    request_type
    )      
    SELECT mps_hr_minimaster_tracker.booking_id,
    mps_hr_minimaster_tracker.item_id,
    mps_hr_minimaster_tracker.candidate_id,
    trim(booking.client_identifier),
    v_now,
    C_REQUEST_TYPE_LEAVER
    FROM mps_hr_minimaster_tracker    
    INNER JOIN booking ON booking.booking_id = mps_hr_minimaster_tracker.booking_id      
    INNER JOIN extension on booking.booking_id = extension.booking_id and extension.marked_deleted = 'N' and extension.active = 'Y'            
    WHERE mps_hr_minimaster_tracker.process_Status = C_HRMINIMASTER_MPS_PROCESSED
    AND mps_hr_minimaster_tracker.process_date < v_now 
    AND mps_hr_minimaster_tracker.request_type != C_REQUEST_TYPE_LEAVER -- previously NOT Leavers
    AND (TRUNC(cast(extension.new_end_Date AS DATE))  <= TRUNC(SYSDATE) OR  -- Either End of contract reached OR booking Cancelled or Closed
         booking.booking_status1_lookup_val_id = 583 AND booking_status2_lookup_val_id in (581, 582)  -- Booking statuses of Closed (583) and Completed (582) / Closed (583) and Cancelled(581)
        );              

    MERGE INTO mps_hr_minimaster_tracker 
    USING  mps_hr_minimaster_gtt 
    ON (mps_hr_minimaster_tracker.booking_id = mps_hr_minimaster_gtt.booking_id AND mps_hr_minimaster_gtt.request_type = C_REQUEST_TYPE_LEAVER )
    WHEN MATCHED THEN UPDATE SET    
        process_status = C_HRMINIMASTER_MPS_PROCESSING,
        request_type   = mps_hr_minimaster_gtt.request_type;
      
    p_rows_to_process_amendment :=  SQL%ROWCOUNT;      
    COMMIT;
    
    -- Amendments Part 2: Amendments    
    INSERT INTO mps_hr_minimaster_gtt (
    booking_id, 
    item_id, 
    candidate_id,
    ni_number,
    mps_warrant_number,    
    supervisor_id,
    temporary_grade,    
    effectve_date_of_change,    
    request_type
    )    
    SELECT mps_hr_minimaster_tracker.booking_id,
    mps_hr_minimaster_tracker.item_id,
    mps_hr_minimaster_tracker.candidate_id,
    trim(candidate.ni_number),
    trim(booking.client_identifier),    
    COALESCE(trim(client.company_name), ' '),    
    C_TEMPORARY_GRADE,    
    v_now,
    C_REQUEST_TYPE_AMENDMENT
    FROM mps_hr_minimaster_tracker    
    INNER JOIN booking ON booking.booking_id = mps_hr_minimaster_tracker.booking_id          
    INNER JOIN candidate ON mps_hr_minimaster_tracker.candidate_id = candidate.cand_id
      LEFT JOIN role additional_authoriser_role ON booking.additional_auth_tier1_role_id  = additional_authoriser_role.role_id AND additional_authoriser_role.marked_deleted  = 'N'       
      LEFT JOIN person ON person.person_id = additional_authoriser_role.person_id AND person.marked_deleted = 'N'
      LEFT JOIN client ON additional_authoriser_role.person_id = client.person_id and additional_authoriser_role.org_id = client.org_id and client.marked_Deleted = 'N'                              
    WHERE mps_hr_minimaster_tracker.process_Status = C_HRMINIMASTER_MPS_PROCESSED
    AND mps_hr_minimaster_tracker.process_date < v_now 
    AND mps_hr_minimaster_tracker.request_type != C_REQUEST_TYPE_LEAVER      
    AND booking.client_identifier IS NOT NULL  -- MPS Warrant Number needs to be populated for an Amendment
    AND COALESCE(mps_hr_minimaster_tracker.supervisor_id, ' ') !=  COALESCE(trim(client.company_name), ' '); 

    MERGE INTO mps_hr_minimaster_tracker 
    USING  mps_hr_minimaster_gtt 
    ON (mps_hr_minimaster_tracker.booking_id = mps_hr_minimaster_gtt.booking_id AND mps_hr_minimaster_gtt.request_type = C_REQUEST_TYPE_AMENDMENT )
    WHEN MATCHED THEN UPDATE SET    
        process_status = C_HRMINIMASTER_MPS_PROCESSING,
        request_type   = mps_hr_minimaster_gtt.request_type;

    p_rows_to_process_amendment := p_rows_to_process_amendment + SQL%ROWCOUNT;
    
    COMMIT;    
            
  END PopulateExtractTrackingTable;
  
BEGIN
  -- Initialization section of package body
  populate_session_variables ;
END MPS_INTEGRATION ;
/
