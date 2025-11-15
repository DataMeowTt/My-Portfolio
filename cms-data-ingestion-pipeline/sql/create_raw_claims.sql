CREATE OR REPLACE TABLE HEALTHCARE_DWH.RAW.CLAIMS_INPATIENT (
    beneficiary_id VARCHAR(50),                             -- BENE_ID
    claim_id VARCHAR(50),                                   -- CLM_ID
    near_line_record_code VARCHAR(10),                      -- NCH_NEAR_LINE_REC_IDENT_CD
    claim_type_code VARCHAR(10),                            -- NCH_CLM_TYPE_CD
    claim_start_date DATE,                                  -- CLM_FROM_DT
    claim_end_date DATE,                                    -- CLM_THRU_DT
    weekly_processing_date DATE,                            -- NCH_WKLY_PROC_DT
    fiscal_processing_date DATE,                            -- FI_CLM_PROC_DT
    claim_query_code VARCHAR(10),                           -- CLAIM_QUERY_CODE
    provider_number VARCHAR(50),                            -- PRVDR_NUM
    facility_type_code VARCHAR(10),                         -- CLM_FAC_TYPE_CD
    service_classification_code VARCHAR(10),                -- CLM_SRVC_CLSFCTN_TYPE_CD
    claim_frequency_code VARCHAR(10),                       -- CLM_FREQ_CD
    fiscal_intermediary_number VARCHAR(50),                 -- FI_NUM
    medicare_non_payment_reason_code VARCHAR(10),           -- CLM_MDCR_NON_PMT_RSN_CD
    claim_payment_amount NUMERIC(18, 2),                    -- CLM_PMT_AMT
    primary_payer_payment_amount NUMERIC(18, 2),            -- NCH_PRMRY_PYR_CLM_PD_AMT
    primary_payer_code VARCHAR(10),                         -- NCH_PRMRY_PYR_CD
    claim_action_code VARCHAR(10),                          -- FI_CLM_ACTN_CD
    provider_state_code VARCHAR(10),                        -- PRVDR_STATE_CD
    organization_npi_number VARCHAR(50),                    -- ORG_NPI_NUM
    attending_physician_upin VARCHAR(50),                   -- AT_PHYSN_UPIN
    attending_physician_npi VARCHAR(50),                    -- AT_PHYSN_NPI
    operating_physician_upin VARCHAR(50),                   -- OP_PHYSN_UPIN
    operating_physician_npi VARCHAR(50),                    -- OP_PHYSN_NPI
    other_physician_upin VARCHAR(50),                       -- OT_PHYSN_UPIN
    other_physician_npi VARCHAR(50),                        -- OT_PHYSN_NPI
    managed_care_plan_switch VARCHAR(10),                   -- CLM_MCO_PD_SW
    patient_discharge_status_code VARCHAR(10),              -- PTNT_DSCHRG_STUS_CD
    pps_indicator_code VARCHAR(10),                         -- CLM_PPS_IND_CD
    total_charge_amount NUMERIC(18, 2),                     -- CLM_TOT_CHRG_AMT
    admission_date DATE,                                    -- CLM_ADMSN_DT
    admission_type_code VARCHAR(10),                        -- CLM_IP_ADMSN_TYPE_CD
    admission_source_code VARCHAR(10),                      -- CLM_SRC_IP_ADMSN_CD
    patient_status_indicator VARCHAR(10),                   -- NCH_PTNT_STATUS_IND_CD
    pass_through_per_diem_amount NUMERIC(18, 2),            -- CLM_PASS_THRU_PER_DIEM_AMT
    inpatient_deductible_amount NUMERIC(18, 2),             -- NCH_BENE_IP_DDCTBL_AMT
    coinsurance_liability_amount NUMERIC(18, 2),            -- NCH_BENE_PTA_COINSRNC_LBLTY_AM
    blood_deductible_amount NUMERIC(18, 2),                 -- NCH_BENE_BLOOD_DDCTBL_LBLTY_AM
    professional_component_charge_amount NUMERIC(18, 2),    -- NCH_PROFNL_CMPNT_CHRG_AMT
    non_covered_charge_amount NUMERIC(18, 2),               -- NCH_IP_NCVRD_CHRG_AMT
    total_deduction_amount NUMERIC(18, 2),                  -- NCH_IP_TOT_DDCTN_AMT
    total_pps_capital_amount NUMERIC(18, 2),                -- CLM_TOT_PPS_CPTL_AMT
    pps_capital_federal_specific_amount NUMERIC(18, 2),     -- CLM_PPS_CPTL_FSP_AMT
    pps_capital_outlier_amount NUMERIC(18, 2),              -- CLM_PPS_CPTL_OUTLIER_AMT
    pps_capital_disproportionate_share_amount NUMERIC(18, 2), -- CLM_PPS_CPTL_DSPRPRTNT_SHR_AMT
    pps_capital_indirect_medical_education_amount NUMERIC(18, 2), -- CLM_PPS_CPTL_IME_AMT
    pps_capital_exception_payment_amount NUMERIC(18, 2),    -- CLM_PPS_CPTL_EXCPTN_AMT
    pps_old_capital_hold_harmless_amount NUMERIC(18, 2),    -- CLM_PPS_OLD_CPTL_HLD_HRMLS_AMT
    pps_drg_weight NUMERIC(18, 4),                          -- CLM_PPS_CPTL_DRG_WT_NUM
    utilization_day_count INT,                          -- CLM_UTLZTN_DAY_CNT
    total_coinsurance_days_count INT,                   -- BENE_TOT_COINSRNC_DAYS_CNT
    lifetime_reserve_days_used INT,                     -- BENE_LRD_USED_CNT
    non_utilization_days_count INT,                     -- CLM_NON_UTLZTN_DAYS_CNT
    blood_units_furnished_quantity INT,                 -- NCH_BLOOD_PNTS_FRNSHD_QTY
    uncovered_stay_start_date DATE,                         -- NCH_VRFD_NCVRD_STAY_FROM_DT
    uncovered_stay_end_date DATE,                           -- NCH_VRFD_NCVRD_STAY_THRU_DT
    covered_care_end_date DATE,                             -- NCH_ACTV_OR_CVRD_LVL_CARE_THRU
    benefits_exhaustion_date DATE,                          -- NCH_BENE_MDCR_BNFTS_EXHTD_DT_I
    patient_discharge_date DATE,                             -- NCH_BENE_DSCHRG_DT
    drg_code VARCHAR(10),                                   -- CLM_DRG_CD
    drg_outlier_stay_code VARCHAR(10),                      -- CLM_DRG_OUTLIER_STAY_CD
    approved_drg_outlier_payment_amount NUMERIC(18, 2),     -- NCH_DRG_OUTLIER_APRVD_PMT_AMT
    admitting_diagnosis_code VARCHAR(10),                   -- ADMTG_DGNS_CD
    principal_diagnosis_code VARCHAR(10),                   -- PRNCPAL_DGNS_CD
    icd_diagnosis_code1 VARCHAR(10),                        -- ICD_DGNS_CD1
    poa_indicator1 VARCHAR(1),                              -- CLM_POA_IND_SW1
    icd_diagnosis_code2 VARCHAR(10),                        -- ICD_DGNS_CD2
    poa_indicator2 VARCHAR(1),                              -- CLM_POA_IND_SW2
    icd_diagnosis_code3 VARCHAR(10),                        -- ICD_DGNS_CD3
    poa_indicator3 VARCHAR(1),                              -- CLM_POA_IND_SW3
    icd_diagnosis_code4 VARCHAR(10),                        -- ICD_DGNS_CD4
    poa_indicator4 VARCHAR(1),                              -- CLM_POA_IND_SW4
    icd_diagnosis_code5 VARCHAR(10),                        -- ICD_DGNS_CD5
    poa_indicator5 VARCHAR(1),                              -- CLM_POA_IND_SW5
    icd_diagnosis_code6 VARCHAR(10),                        -- ICD_DGNS_CD6
    poa_indicator6 VARCHAR(1),                              -- CLM_POA_IND_SW6
    icd_diagnosis_code7 VARCHAR(10),                        -- ICD_DGNS_CD7
    poa_indicator7 VARCHAR(1),                              -- CLM_POA_IND_SW7
    icd_diagnosis_code8 VARCHAR(10),                        -- ICD_DGNS_CD8
    poa_indicator8 VARCHAR(1),                              -- CLM_POA_IND_SW8
    icd_diagnosis_code9 VARCHAR(10),                        -- ICD_DGNS_CD9
    poa_indicator9 VARCHAR(1),                              -- CLM_POA_IND_SW9
    icd_diagnosis_code10 VARCHAR(10),                       -- ICD_DGNS_CD10
    poa_indicator10 VARCHAR(1),                             -- CLM_POA_IND_SW10
    icd_diagnosis_code11 VARCHAR(10),                       -- ICD_DGNS_CD11
    poa_indicator11 VARCHAR(1),                             -- CLM_POA_IND_SW11
    icd_diagnosis_code12 VARCHAR(10),                       -- ICD_DGNS_CD12
    poa_indicator12 VARCHAR(1),                             -- CLM_POA_IND_SW12
    icd_diagnosis_code13 VARCHAR(10),                       -- ICD_DGNS_CD13
    poa_indicator13 VARCHAR(1),                             -- CLM_POA_IND_SW13
    icd_diagnosis_code14 VARCHAR(10),                       -- ICD_DGNS_CD14
    poa_indicator14 VARCHAR(1),                             -- CLM_POA_IND_SW14
    icd_diagnosis_code15 VARCHAR(10),                       -- ICD_DGNS_CD15
    poa_indicator15 VARCHAR(1),                             -- CLM_POA_IND_SW15
    icd_diagnosis_code16 VARCHAR(10),                       -- ICD_DGNS_CD16
    poa_indicator16 VARCHAR(1),                             -- CLM_POA_IND_SW16
    icd_diagnosis_code17 VARCHAR(10),                       -- ICD_DGNS_CD17
    poa_indicator17 VARCHAR(1),                             -- CLM_POA_IND_SW17
    icd_diagnosis_code18 VARCHAR(10),                       -- ICD_DGNS_CD18
    poa_indicator18 VARCHAR(1),                             -- CLM_POA_IND_SW18
    icd_diagnosis_code19 VARCHAR(10),                       -- ICD_DGNS_CD19
    poa_indicator19 VARCHAR(1),                             -- CLM_POA_IND_SW19
    icd_diagnosis_code20 VARCHAR(10),                       -- ICD_DGNS_CD20
    poa_indicator20 VARCHAR(1),                             -- CLM_POA_IND_SW20
    icd_diagnosis_code21 VARCHAR(10),                       -- ICD_DGNS_CD21
    poa_indicator21 VARCHAR(1),                             -- CLM_POA_IND_SW21
    icd_diagnosis_code22 VARCHAR(10),                       -- ICD_DGNS_CD22
    poa_indicator22 VARCHAR(1),                             -- CLM_POA_IND_SW22
    icd_diagnosis_code23 VARCHAR(10),                       -- ICD_DGNS_CD23
    poa_indicator23 VARCHAR(1),                             -- CLM_POA_IND_SW23
    icd_diagnosis_code24 VARCHAR(10),                       -- ICD_DGNS_CD24
    poa_indicator24 VARCHAR(1),                             -- CLM_POA_IND_SW24
    icd_diagnosis_code25 VARCHAR(10),                       -- ICD_DGNS_CD25
    poa_indicator25 VARCHAR(1),                             -- CLM_POA_IND_SW25,
    first_external_diagnosis_code VARCHAR(10),              -- FST_DGNS_E_CD
    external_diagnosis_code1 VARCHAR(10),                   -- ICD_DGNS_E_CD1
    external_poa_indicator1 VARCHAR(1),                     -- CLM_E_POA_IND_SW1
    external_diagnosis_code2 VARCHAR(10),                   -- ICD_DGNS_E_CD2
    external_poa_indicator2 VARCHAR(1),                     -- CLM_E_POA_IND_SW2
    external_diagnosis_code3 VARCHAR(10),                   -- ICD_DGNS_E_CD3
    external_poa_indicator3 VARCHAR(1),                     -- CLM_E_POA_IND_SW3
    external_diagnosis_code4 VARCHAR(10),                   -- ICD_DGNS_E_CD4
    external_poa_indicator4 VARCHAR(1),                     -- CLM_E_POA_IND_SW4
    external_diagnosis_code5 VARCHAR(10),                   -- ICD_DGNS_E_CD5
    external_poa_indicator5 VARCHAR(1),                     -- CLM_E_POA_IND_SW5
    external_diagnosis_code6 VARCHAR(10),                   -- ICD_DGNS_E_CD6
    external_poa_indicator6 VARCHAR(1),                     -- CLM_E_POA_IND_SW6
    external_diagnosis_code7 VARCHAR(10),                   -- ICD_DGNS_E_CD7
    external_poa_indicator7 VARCHAR(1),                     -- CLM_E_POA_IND_SW7
    external_diagnosis_code8 VARCHAR(10),                   -- ICD_DGNS_E_CD8
    external_poa_indicator8 VARCHAR(1),                     -- CLM_E_POA_IND_SW8
    external_diagnosis_code9 VARCHAR(10),                   -- ICD_DGNS_E_CD9
    external_poa_indicator9 VARCHAR(1),                     -- CLM_E_POA_IND_SW9
    external_diagnosis_code10 VARCHAR(10),                  -- ICD_DGNS_E_CD10
    external_poa_indicator10 VARCHAR(1),                    -- CLM_E_POA_IND_SW10
    external_diagnosis_code11 VARCHAR(10),                  -- ICD_DGNS_E_CD11
    external_poa_indicator11 VARCHAR(1),                    -- CLM_E_POA_IND_SW11
    external_diagnosis_code12 VARCHAR(10),                  -- ICD_DGNS_E_CD12
    external_poa_indicator12 VARCHAR(1),                    -- CLM_E_POA_IND_SW12
    icd_procedure_code1 VARCHAR(10),                        -- ICD_PRCDR_CD1
    procedure_date1 DATE,                                   -- PRCDR_DT1
    icd_procedure_code2 VARCHAR(10),                        -- ICD_PRCDR_CD2
    procedure_date2 DATE,                                   -- PRCDR_DT2
    icd_procedure_code3 VARCHAR(10),                        -- ICD_PRCDR_CD3
    procedure_date3 DATE,                                   -- PRCDR_DT3
    icd_procedure_code4 VARCHAR(10),                        -- ICD_PRCDR_CD4
    procedure_date4 DATE,                                   -- PRCDR_DT4
    icd_procedure_code5 VARCHAR(10),                        -- ICD_PRCDR_CD5
    procedure_date5 DATE,                                   -- PRCDR_DT5
    icd_procedure_code6 VARCHAR(10),                        -- ICD_PRCDR_CD6
    procedure_date6 DATE,                                   -- PRCDR_DT6
    icd_procedure_code7 VARCHAR(10),                        -- ICD_PRCDR_CD7
    procedure_date7 DATE,                                   -- PRCDR_DT7
    icd_procedure_code8 VARCHAR(10),                        -- ICD_PRCDR_CD8
    procedure_date8 DATE,                                   -- PRCDR_DT8
    icd_procedure_code9 VARCHAR(10),                        -- ICD_PRCDR_CD9
    procedure_date9 DATE,                                   -- PRCDR_DT9
    icd_procedure_code10 VARCHAR(10),                       -- ICD_PRCDR_CD10
    procedure_date10 DATE,                                  -- PRCDR_DT10
    icd_procedure_code11 VARCHAR(10),                       -- ICD_PRCDR_CD11
    procedure_date11 DATE,                                  -- PRCDR_DT11
    icd_procedure_code12 VARCHAR(10),                       -- ICD_PRCDR_CD12
    procedure_date12 DATE,                                  -- PRCDR_DT12
    icd_procedure_code13 VARCHAR(10),                       -- ICD_PRCDR_CD13
    procedure_date13 DATE,                                  -- PRCDR_DT13
    icd_procedure_code14 VARCHAR(10),                       -- ICD_PRCDR_CD14
    procedure_date14 DATE,                                  -- PRCDR_DT14
    icd_procedure_code15 VARCHAR(10),                       -- ICD_PRCDR_CD15
    procedure_date15 DATE,                                  -- PRCDR_DT15
    icd_procedure_code16 VARCHAR(10),                       -- ICD_PRCDR_CD16
    procedure_date16 DATE,                                  -- PRCDR_DT16
    icd_procedure_code17 VARCHAR(10),                       -- ICD_PRCDR_CD17
    procedure_date17 DATE,                                  -- PRCDR_DT17
    icd_procedure_code18 VARCHAR(10),                       -- ICD_PRCDR_CD18
    procedure_date18 DATE,                                  -- PRCDR_DT18
    icd_procedure_code19 VARCHAR(10),                       -- ICD_PRCDR_CD19
    procedure_date19 DATE,                                  -- PRCDR_DT19
    icd_procedure_code20 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date20 DATE,                                  -- PRCDR_DT20
    icd_procedure_code21 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date21 DATE,                                  -- PRCDR_DT20
    icd_procedure_code22 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date22 DATE,                                  -- PRCDR_DT20
    icd_procedure_code23 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date23 DATE,                                  -- PRCDR_DT20
    icd_procedure_code24 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date24 DATE,                                  -- PRCDR_DT20
    icd_procedure_code25 VARCHAR(10),                       -- ICD_PRCDR_CD20
    procedure_date25 DATE,                                  -- PRCDR_DT20
    IME_OP_CLM_VAL_AMT varchar(100),
    DSH_OP_CLM_VAL_AMT varchar(100),
    uncompensated_care_payment_amount NUMERIC(18, 2),       -- CLM_UNCOMPD_CARE_PMT_AMT
    line_number VARCHAR(10),                                -- CLM_LINE_NUM
    revenue_center_code VARCHAR(10),                        -- REV_CNTR
    hcpcs_code VARCHAR(10),                                 -- HCPCS_CD
    deductible_coinsurance_code VARCHAR(10),                -- REV_CNTR_DDCTBL_COINSRNC_CD
    source_file_name VARCHAR(255),                          -- Source file name
    loaded_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP   
);
