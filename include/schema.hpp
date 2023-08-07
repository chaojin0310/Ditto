/*
 * Copyright (c) Computer Systems Research Group @PKU (chao jin)

 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree. 
 */

#ifndef SCHEMA_HPP
#define SCHEMA_HPP

#include <stdlib.h>
#include <string.h>

#define MAX_COLUMNS 44

/* data type of the column */
enum class DType {
    Int64,
    Float32,
    String,
    Date
};

struct Schema {
    int num_cols;
    int int_columns;
    int float_columns;
    int string_columns;
    int date_columns;
    DType dtypes[MAX_COLUMNS];
};

/* Schema Type */
#define web_sales 0
#define web_returns 1
#define date_dim 2
#define customer_address 3
#define web_site 4
#define int_list 5
#define q95_s1 6
#define q95_s2 7
#define q95_s3 8
#define q95_s4 9
#define q95_s7 10
#define q95_s8_web 11
#define q95_s8 12
#define q94_s1 13
#define q94_s2 14
#define q94_s3 15
#define q94_s4 16
#define q94_s7 17
#define q94_s8_web 18
#define q94_s8 19
#define catalog_sales 20
#define call_center 21
#define q16_s1 22
#define q16_s2 23
#define q16_s3 24
#define q16_s4 25
#define q16_s7 26
#define q16_s8_cc 27
#define q16_s8 28
#define catalog_returns 29
#define store_table 30
#define store_returns 31
#define customer_table 32
#define q1_s2 33
#define q1_s3 34  // customer_total_return
#define q1_s6 35
#define q1_s7 36
#define float_list 37 
#define q1_s10 38  // string list

/* Column names and their column index of the table data area */
// web_sales schema
#define ws_sold_date_sk 0
#define ws_sold_time_sk 1
#define ws_ship_date_sk 2
#define ws_item_sk 3
#define ws_bill_customer_sk 4
#define ws_bill_cdemo_sk 5
#define ws_bill_hdemo_sk 6
#define ws_bill_addr_sk 7
#define ws_ship_customer_sk 8
#define ws_ship_cdemo_sk 9
#define ws_ship_hdemo_sk 10
#define ws_ship_addr_sk 11
#define ws_web_page_sk 12
#define ws_web_site_sk 13
#define ws_ship_mode_sk 14
#define ws_warehouse_sk 15
#define ws_promo_sk 16
#define ws_order_number 17
#define ws_quantity 18
#define ws_wholesale_cost 0
#define ws_list_price 1
#define ws_sales_price 2
#define ws_ext_discount_amt 3
#define ws_ext_sales_price 4
#define ws_ext_wholesale_cost 5
#define ws_ext_list_price 6
#define ws_ext_tax 7
#define ws_coupon_amt 8
#define ws_ext_ship_cost 9
#define ws_net_paid 10
#define ws_net_paid_inc_tax 11
#define ws_net_paid_inc_ship 12
#define ws_net_paid_inc_ship_tax 13
#define ws_net_profit 14

// web_returns schema
#define wr_returned_date_sk 0
#define wr_returned_time_sk 1
#define wr_item_sk 2
#define wr_refunded_customer_sk 3
#define wr_refunded_cdemo_sk 4
#define wr_refunded_hdemo_sk 5
#define wr_refunded_addr_sk 6
#define wr_returning_customer_sk 7
#define wr_returning_cdemo_sk 8
#define wr_returning_hdemo_sk 9
#define wr_returning_addr_sk 10
#define wr_web_page_sk 11
#define wr_reason_sk 12
#define wr_order_number 13
#define wr_return_quantity 14
#define wr_return_amt 0
#define wr_return_tax 1
#define wr_return_amt_inc_tax 2
#define wr_fee 3
#define wr_return_ship_cost 4
#define wr_refunded_cash 5
#define wr_reversed_charge 6
#define wr_account_credit 7
#define wr_net_loss 8

// date_dim schema
#define d_date_sk 0
#define d_date_id 0
#define d_date 0
#define d_month_seq 1
#define d_week_seq 2
#define d_quarter_seq 3
#define d_year 4
#define d_dow 5
#define d_moy 6
#define d_dom 7
#define d_qoy 8
#define d_fy_year 9
#define d_fy_quarter_seq 10
#define d_fy_week_seq 11
#define d_day_name 1
#define d_quarter_name 2
#define d_holiday 3
#define d_weekend 4
#define d_following_holiday 5
#define d_first_dom 12
#define d_last_dom 13
#define d_same_day_ly 14
#define d_same_day_lq 15
#define d_current_day 6
#define d_current_week 7
#define d_current_month 8
#define d_current_quarter 9
#define d_current_year 10

// customer_address schema
#define ca_address_sk 0
#define ca_address_id 0
#define ca_street_number 1
#define ca_street_name 2
#define ca_street_type 3
#define ca_suite_number 4
#define ca_city 5
#define ca_county 6
#define ca_state 7
#define ca_zip 8
#define ca_country 9
#define ca_gmt_offset 0
#define ca_location_type 10

// web_site schema
#define web_site_sk 0
#define web_site_id 0
#define web_rec_start_date 0
#define web_rec_end_date 1
#define web_name 1
#define web_open_date_sk 1
#define web_close_date_sk 2
#define web_class 2
#define web_manager 3
#define web_mkt_id 3
#define web_mkt_class 4
#define web_mkt_desc 5
#define web_market_manager 6
#define web_company_id 4
#define web_company_name 7
#define web_street_number 8
#define web_street_name 9
#define web_street_type 10
#define web_suite_number 11
#define web_city 12
#define web_county 13
#define web_state 14
#define web_zip 15
#define web_country 16
#define web_gmt_offset 0
#define web_tax_percentage 1

// catalog_sales schema
#define cs_sold_date_sk 0
#define cs_sold_time_sk 1
#define cs_ship_date_sk 2
#define cs_bill_customer_sk 3
#define cs_bill_cdemo_sk 4
#define cs_bill_hdemo_sk 5
#define cs_bill_addr_sk 6
#define cs_ship_customer_sk 7
#define cs_ship_cdemo_sk 8
#define cs_ship_hdemo_sk 9
#define cs_ship_addr_sk 10
#define cs_call_center_sk 11
#define cs_catalog_page_sk 12
#define cs_ship_mode_sk 13
#define cs_warehouse_sk 14
#define cs_item_sk 15
#define cs_promo_sk 16
#define cs_order_number 17
#define cs_quantity 18
#define cs_wholesale_cost 0
#define cs_list_price 1
#define cs_sales_price 2
#define cs_ext_discount_amt 3
#define cs_ext_sales_price 4
#define cs_ext_wholesale_cost 5
#define cs_ext_list_price 6
#define cs_ext_tax 7
#define cs_coupon_amt 8
#define cs_ext_ship_cost 9
#define cs_net_paid 10
#define cs_net_paid_inc_tax 11
#define cs_net_paid_inc_ship 12
#define cs_net_paid_inc_ship_tax 13
#define cs_net_profit 14

// call_center schema
#define cc_call_center_sk 0
#define cc_call_center_id 0
#define cc_rec_start_date 0
#define cc_rec_end_date 1
#define cc_closed_date_sk 1
#define cc_open_date_sk 2
#define cc_name 1
#define cc_class 2
#define cc_employees 3
#define cc_sq_ft 4
#define cc_hours 3
#define cc_manager 4
#define cc_mkt_id 5
#define cc_mkt_class 5
#define cc_mkt_desc 6
#define cc_market_manager 7
#define cc_division 6
#define cc_division_name 8
#define cc_company 7
#define cc_company_name 9
#define cc_street_number 10
#define cc_street_name 11
#define cc_street_type 12
#define cc_suite_number 13
#define cc_city 14
#define cc_county 15
#define cc_state 16
#define cc_zip 17
#define cc_country 18
#define cc_gmt_offset 0
#define cc_tax_percentage 1

// catalog_returns schema
#define cr_returned_date_sk 0
#define cr_returned_time_sk 1
#define cr_item_sk 2
#define cr_refunded_customer_sk 3
#define cr_refunded_cdemo_sk 4
#define cr_refunded_hdemo_sk 5
#define cr_refunded_addr_sk 6
#define cr_returning_customer_sk 7
#define cr_returning_cdemo_sk 8
#define cr_returning_hdemo_sk 9
#define cr_returning_addr_sk 10
#define cr_call_center_sk 11
#define cr_catalog_page_sk 12
#define cr_ship_mode_sk 13
#define cr_warehouse_sk 14
#define cr_reason_sk 15
#define cr_order_number 16
#define cr_return_quantity 17
#define cr_return_amount 0
#define cr_return_tax 1
#define cr_return_amt_inc_tax 2
#define cr_fee 3
#define cr_return_ship_cost 4
#define cr_refunded_cash 5
#define cr_reversed_charge 6
#define cr_store_credit 7
#define cr_net_loss 8

// store schema
#define s_store_sk 0
#define s_store_id 0
#define s_rec_start_date 0
#define s_rec_end_date 1
#define s_closed_date_sk 1
#define s_store_name 1
#define s_number_employees 2
#define s_floor_space 3
#define s_hours 2
#define s_manager 3
#define s_market_id 4
#define s_geography_class 4
#define s_market_desc 5
#define s_market_manager 6
#define s_division_id 5
#define s_division_name 7
#define s_company_id 6
#define s_company_name 8
#define s_street_number 9
#define s_street_name 10
#define s_street_type 11
#define s_suite_number 12
#define s_city 13
#define s_county 14
#define s_state 15
#define s_zip 16
#define s_country 17
#define s_gmt_offset 0
#define s_tax_precentage 1

// store_returns schema
#define sr_returned_date_sk 0
#define sr_return_time_sk 1
#define sr_item_sk 2
#define sr_customer_sk 3
#define sr_cdemo_sk 4
#define sr_hdemo_sk 5
#define sr_addr_sk 6
#define sr_store_sk 7
#define sr_reason_sk 8
#define sr_ticket_number 9
#define sr_return_quantity 10
#define sr_return_amt 0
#define sr_return_tax 1
#define sr_return_amt_inc_tax 2
#define sr_fee 3
#define sr_return_ship_cost 4
#define sr_refunded_cash 5
#define sr_reversed_charge 6
#define sr_store_credit 7
#define sr_net_loss 8

// customer schema
#define c_customer_sk 0
#define c_customer_id 0
#define c_current_cdemo_sk 1
#define c_current_hdemo_sk 2
#define c_current_addr_sk 3
#define c_first_shipto_date_sk 4
#define c_first_sales_date_sk 5
#define c_salutation 1
#define c_first_name 2
#define c_last_name 3
#define c_preferred_cust_flag 4
#define c_birth_day 6
#define c_birth_month 7
#define c_birth_year 8
#define c_birth_country 5
#define c_login 6
#define c_email_address 7
#define c_last_review_date 9

// q95_s1 schema
#define q95_s1_ws_order_number 0
#define q95_s1_ws_warehouse_sk 1

// q95_s2 schema
#define q95_s2_ws_order_number 0
#define q95_s2_ws_warehouse_sk_count 1
#define q95_s2_ws_warehouse_sk 2

//q95_s3 schema
#define q95_s3_ws_order_number 0
#define q95_s3_ws_warehouse_sk_count 1

// q95_s4 schema
#define q95_s4_ws_order_number 0
#define q95_s4_ws_ext_ship_cost 0
#define q95_s4_ws_net_profit 1
#define q95_s4_ws_ship_date_sk 1
#define q95_s4_ws_ship_addr_sk 2
#define q95_s4_ws_web_site_sk 3
#define q95_s4_ws_warehouse_sk 4

// q95_s7 schema
#define q95_s7_ca_address_sk 0
#define q95_s7_ca_state 0

// q95_s8_web schema
#define q95_s8_web_web_site_sk 0
#define q95_s8_web_web_company_name 0

// q95_s8 schema
#define q95_s8_ws_order_number 0
#define q95_s8_ws_ext_ship_cost 0
#define q95_s8_ws_net_profit 1

// q94_s1 schema
#define q94_s1_ws_order_number 0
#define q94_s1_ws_warehouse_sk 1

// q94_s2 schema
#define q94_s2_ws_order_number 0
#define q94_s2_ws_warehouse_sk_count 1
#define q94_s2_ws_warehouse_sk 2

//q94_s3 schema
#define q94_s3_ws_order_number 0
#define q94_s3_ws_warehouse_sk_count 1

// q94_s4 schema
#define q94_s4_ws_order_number 0
#define q94_s4_ws_ext_ship_cost 0
#define q94_s4_ws_net_profit 1
#define q94_s4_ws_ship_date_sk 1
#define q94_s4_ws_ship_addr_sk 2
#define q94_s4_ws_web_site_sk 3
#define q94_s4_ws_warehouse_sk 4

// q94_s7 schema
#define q94_s7_ca_address_sk 0
#define q94_s7_ca_state 0

// q94_s8_web schema
#define q94_s8_web_web_site_sk 0
#define q94_s8_web_web_company_name 0

// q94_s8 schema
#define q94_s8_ws_order_number 0
#define q94_s8_ws_ext_ship_cost 0
#define q94_s8_ws_net_profit 1

// q16_s1 schema
#define q16_s1_cs_order_number 0
#define q16_s1_cs_warehouse_sk 1

// q16_s2 schema
#define q16_s2_cs_order_number 0
#define q16_s2_cs_warehouse_sk_count 1
#define q16_s2_cs_warehouse_sk 2

// q16_s3 schema
#define q16_s3_cs_order_number 0
#define q16_s3_cs_warehouse_sk_count 1

// q16_s4 schema
#define q16_s4_cs_order_number 0
#define q16_s4_cs_ext_ship_cost 0
#define q16_s4_cs_net_profit 1
#define q16_s4_cs_ship_date_sk 1
#define q16_s4_cs_ship_addr_sk 2
#define q16_s4_cs_call_center_sk 3
#define q16_s4_cs_warehouse_sk 4

// q16_s7 schema
#define q16_s7_ca_address_sk 0
#define q16_s7_ca_state 0

// q16_s8_cc schema
#define q16_s8_cc_cc_call_center_sk 0
#define q16_s8_cc_cc_county 0

// q16_s8 schema
#define q16_s8_cs_order_number 0
#define q16_s8_cs_ext_ship_cost 0
#define q16_s8_cs_net_profit 1

// q1_s2 schema
#define q1_s2_sr_customer_sk 0
#define q1_s2_sr_store_sk 1
#define q1_s2_sr_fee 0
#define q1_s2_sr_sr_returned_date_sk 2

// q1_s3 schema
#define q1_s3_ctr_customer_sk 0
#define q1_s3_ctr_store_sk 1
#define q1_s3_ctr_total_return 0

// q1_s6 schema
#define q1_s6_c_customer_sk 0
#define q1_s6_c_customer_id 0

// q1_s7 schema
#define q1_s7_c_customer_id 0
#define q1_s7_ctr_customer_sk 0
#define q1_s7_ctr_store_sk 1
#define q1_s7_ctr_total_return 0

// q1_s10 schema
#define q1_s10_c_customer_id 0

void init_schema(struct Schema &schema, int schema_type) {
    switch (schema_type) {
        case web_sales:
            schema.num_cols = 34;
            schema.int_columns = 19;
            schema.float_columns = 15;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::Int64;
            schema.dtypes[15] = DType::Int64;
            schema.dtypes[16] = DType::Int64;
            schema.dtypes[17] = DType::Int64;
            schema.dtypes[18] = DType::Int64;
            schema.dtypes[19] = DType::Float32;
            schema.dtypes[20] = DType::Float32;
            schema.dtypes[21] = DType::Float32;
            schema.dtypes[22] = DType::Float32;
            schema.dtypes[23] = DType::Float32;
            schema.dtypes[24] = DType::Float32;
            schema.dtypes[25] = DType::Float32;
            schema.dtypes[26] = DType::Float32;
            schema.dtypes[27] = DType::Float32;
            schema.dtypes[28] = DType::Float32;
            schema.dtypes[29] = DType::Float32;
            schema.dtypes[30] = DType::Float32;
            schema.dtypes[31] = DType::Float32;
            schema.dtypes[32] = DType::Float32;
            schema.dtypes[33] = DType::Float32;
            break;
        case catalog_sales:
            schema.num_cols = 34;
            schema.int_columns = 19;
            schema.float_columns = 15;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::Int64;
            schema.dtypes[15] = DType::Int64;
            schema.dtypes[16] = DType::Int64;
            schema.dtypes[17] = DType::Int64;
            schema.dtypes[18] = DType::Int64;
            schema.dtypes[19] = DType::Float32;
            schema.dtypes[20] = DType::Float32;
            schema.dtypes[21] = DType::Float32;
            schema.dtypes[22] = DType::Float32;
            schema.dtypes[23] = DType::Float32;
            schema.dtypes[24] = DType::Float32;
            schema.dtypes[25] = DType::Float32;
            schema.dtypes[26] = DType::Float32;
            schema.dtypes[27] = DType::Float32;
            schema.dtypes[28] = DType::Float32;
            schema.dtypes[29] = DType::Float32;
            schema.dtypes[30] = DType::Float32;
            schema.dtypes[31] = DType::Float32;
            schema.dtypes[32] = DType::Float32;
            schema.dtypes[33] = DType::Float32;
            break;
        case web_returns:
            schema.num_cols = 24;
            schema.int_columns = 15;
            schema.float_columns = 9;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::Int64;
            schema.dtypes[15] = DType::Float32;
            schema.dtypes[16] = DType::Float32;
            schema.dtypes[17] = DType::Float32;
            schema.dtypes[18] = DType::Float32;
            schema.dtypes[19] = DType::Float32;
            schema.dtypes[20] = DType::Float32;
            schema.dtypes[21] = DType::Float32;
            schema.dtypes[22] = DType::Float32;
            schema.dtypes[23] = DType::Float32;
            break;
        case date_dim:
            schema.num_cols = 28;
            schema.int_columns = 16;
            schema.float_columns = 0;
            schema.string_columns = 11;
            schema.date_columns = 1;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::Date;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::String;
            schema.dtypes[15] = DType::String;
            schema.dtypes[16] = DType::String;
            schema.dtypes[17] = DType::String;
            schema.dtypes[18] = DType::String;
            schema.dtypes[19] = DType::Int64;
            schema.dtypes[20] = DType::Int64;
            schema.dtypes[21] = DType::Int64;
            schema.dtypes[22] = DType::Int64;
            schema.dtypes[23] = DType::String;
            schema.dtypes[24] = DType::String;
            schema.dtypes[25] = DType::String;
            schema.dtypes[26] = DType::String;
            schema.dtypes[27] = DType::String;
            break;
        case customer_address:
            schema.num_cols = 13;
            schema.int_columns = 1;
            schema.float_columns = 1;
            schema.string_columns = 11;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::String;
            schema.dtypes[3] = DType::String;
            schema.dtypes[4] = DType::String;
            schema.dtypes[5] = DType::String;
            schema.dtypes[6] = DType::String;
            schema.dtypes[7] = DType::String;
            schema.dtypes[8] = DType::String;
            schema.dtypes[9] = DType::String;
            schema.dtypes[10] = DType::String;
            schema.dtypes[11] = DType::Float32;
            schema.dtypes[12] = DType::String;
            break;
        case web_site:
            schema.num_cols = 26;
            schema.int_columns = 5;
            schema.float_columns = 2;
            schema.string_columns = 17;
            schema.date_columns = 2;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::Date;
            schema.dtypes[3] = DType::Date;
            schema.dtypes[4] = DType::String;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::String;
            schema.dtypes[8] = DType::String;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::String;
            schema.dtypes[11] = DType::String;
            schema.dtypes[12] = DType::String;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::String;
            schema.dtypes[15] = DType::String;
            schema.dtypes[16] = DType::String;
            schema.dtypes[17] = DType::String;
            schema.dtypes[18] = DType::String;
            schema.dtypes[19] = DType::String;
            schema.dtypes[20] = DType::String;
            schema.dtypes[21] = DType::String;
            schema.dtypes[22] = DType::String;
            schema.dtypes[23] = DType::String;
            schema.dtypes[24] = DType::Float32;
            schema.dtypes[25] = DType::Float32;
            break;
        case call_center:
            schema.num_cols = 31;
            schema.int_columns = 8;
            schema.float_columns = 2;
            schema.string_columns = 19;
            schema.date_columns = 2;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::Date;
            schema.dtypes[3] = DType::Date;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::String;
            schema.dtypes[7] = DType::String;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::String;
            schema.dtypes[11] = DType::String;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::String;
            schema.dtypes[14] = DType::String;
            schema.dtypes[15] = DType::String;
            schema.dtypes[16] = DType::Int64;
            schema.dtypes[17] = DType::String;
            schema.dtypes[18] = DType::Int64;
            schema.dtypes[19] = DType::String;
            schema.dtypes[20] = DType::String;
            schema.dtypes[21] = DType::String;
            schema.dtypes[22] = DType::String;
            schema.dtypes[23] = DType::String;
            schema.dtypes[24] = DType::String;
            schema.dtypes[25] = DType::String;
            schema.dtypes[26] = DType::String;
            schema.dtypes[27] = DType::String;
            schema.dtypes[28] = DType::String;
            schema.dtypes[29] = DType::Float32;
            schema.dtypes[30] = DType::Float32;
            break;
        case catalog_returns:
            schema.num_cols = 27;
            schema.int_columns = 18;
            schema.float_columns = 9;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::Int64;
            schema.dtypes[15] = DType::Int64;
            schema.dtypes[16] = DType::Int64;
            schema.dtypes[17] = DType::Int64;
            schema.dtypes[18] = DType::Float32;
            schema.dtypes[19] = DType::Float32;
            schema.dtypes[20] = DType::Float32;
            schema.dtypes[21] = DType::Float32;
            schema.dtypes[22] = DType::Float32;
            schema.dtypes[23] = DType::Float32;
            schema.dtypes[24] = DType::Float32;
            schema.dtypes[25] = DType::Float32;
            schema.dtypes[26] = DType::Float32;
            break;
        case store_table:
            schema.num_cols = 29;
            schema.int_columns = 7;
            schema.float_columns = 2;
            schema.string_columns = 18;
            schema.date_columns = 2;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::Date;
            schema.dtypes[3] = DType::Date;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::String;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::String;
            schema.dtypes[9] = DType::String;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::String;
            schema.dtypes[12] = DType::String;
            schema.dtypes[13] = DType::String;
            schema.dtypes[14] = DType::Int64;
            schema.dtypes[15] = DType::String;
            schema.dtypes[16] = DType::Int64;
            schema.dtypes[17] = DType::String;
            schema.dtypes[18] = DType::String;
            schema.dtypes[19] = DType::String;
            schema.dtypes[20] = DType::String;
            schema.dtypes[21] = DType::String;
            schema.dtypes[22] = DType::String;
            schema.dtypes[23] = DType::String;
            schema.dtypes[24] = DType::String;
            schema.dtypes[25] = DType::String;
            schema.dtypes[26] = DType::String;
            schema.dtypes[27] = DType::Float32;
            schema.dtypes[28] = DType::Float32;
            break;
        case store_returns:
            schema.num_cols = 20;
            schema.int_columns = 11;
            schema.float_columns = 9;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::Int64;
            schema.dtypes[8] = DType::Int64;
            schema.dtypes[9] = DType::Int64;
            schema.dtypes[10] = DType::Int64;
            schema.dtypes[11] = DType::Float32;
            schema.dtypes[12] = DType::Float32;
            schema.dtypes[13] = DType::Float32;
            schema.dtypes[14] = DType::Float32;
            schema.dtypes[15] = DType::Float32;
            schema.dtypes[16] = DType::Float32;
            schema.dtypes[17] = DType::Float32;
            schema.dtypes[18] = DType::Float32;
            schema.dtypes[19] = DType::Float32;
            break;
        case customer_table:
            schema.num_cols = 18;
            schema.int_columns = 10;
            schema.float_columns = 0;
            schema.string_columns = 8;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            schema.dtypes[7] = DType::String;
            schema.dtypes[8] = DType::String;
            schema.dtypes[9] = DType::String;
            schema.dtypes[10] = DType::String;
            schema.dtypes[11] = DType::Int64;
            schema.dtypes[12] = DType::Int64;
            schema.dtypes[13] = DType::Int64;
            schema.dtypes[14] = DType::String;
            schema.dtypes[15] = DType::String;
            schema.dtypes[16] = DType::String;
            schema.dtypes[17] = DType::Int64;
            break;
        case int_list:
            schema.num_cols = 1;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            break;
        case q95_s1:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q95_s2:
            schema.num_cols = 3;
            schema.int_columns = 3;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q95_s3:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q95_s4:
            schema.num_cols = 7;
            schema.int_columns = 5;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            break;
        case q95_s7:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q95_s8_web:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q95_s8:
            schema.num_cols = 3;
            schema.int_columns = 1;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            break;
        case q94_s1:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q94_s2:
            schema.num_cols = 3;
            schema.int_columns = 3;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q94_s3:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q94_s4:
            schema.num_cols = 7;
            schema.int_columns = 5;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            break;
        case q94_s7:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q94_s8_web:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q94_s8:
            schema.num_cols = 3;
            schema.int_columns = 1;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            break;
        case q16_s1:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q16_s2:
            schema.num_cols = 3;
            schema.int_columns = 3;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q16_s3:
            schema.num_cols = 2;
            schema.int_columns = 2;
            schema.float_columns = 0;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            break;
        case q16_s4:
            schema.num_cols = 7;
            schema.int_columns = 5;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            schema.dtypes[3] = DType::Int64;
            schema.dtypes[4] = DType::Int64;
            schema.dtypes[5] = DType::Int64;
            schema.dtypes[6] = DType::Int64;
            break;
        case q16_s7:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q16_s8_cc:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q16_s8:
            schema.num_cols = 3;
            schema.int_columns = 1;
            schema.float_columns = 2;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Float32;
            schema.dtypes[2] = DType::Float32;
            break;
        case q1_s2:
            schema.num_cols = 4;
            schema.int_columns = 3;
            schema.float_columns = 1;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Float32;
            schema.dtypes[3] = DType::Int64;
            break;
        case q1_s3:
            schema.num_cols = 3;
            schema.int_columns = 2;
            schema.float_columns = 1;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Float32;
            break;
        case q1_s6:
            schema.num_cols = 2;
            schema.int_columns = 1;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Int64;
            schema.dtypes[1] = DType::String;
            break;
        case q1_s7:
            schema.num_cols = 4;
            schema.int_columns = 2;
            schema.float_columns = 1;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::String;
            schema.dtypes[1] = DType::Int64;
            schema.dtypes[2] = DType::Int64;
            schema.dtypes[3] = DType::Float32;  
            break;
        case float_list:
            schema.num_cols = 1;
            schema.int_columns = 0;
            schema.float_columns = 1;
            schema.string_columns = 0;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::Float32;
            break;
        case q1_s10:
            schema.num_cols = 1;
            schema.int_columns = 0;
            schema.float_columns = 0;
            schema.string_columns = 1;
            schema.date_columns = 0;
            memset(schema.dtypes, 0, sizeof(schema.dtypes));
            schema.dtypes[0] = DType::String;
            break;
        default:
            break;
    }
}

#endif /* SCHEMA_HPP */
