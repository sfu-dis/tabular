/*
 * Copyright (C) 2024 Data-Intensive Systems Lab, Simon Fraser University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>
#include <cstring>

namespace noname {

static const std::string create_warehouse_sql =
  R"(CREATE TABLE warehouse (
      w_id       int            NOT NULL,
      w_ytd      float          NOT NULL,
      w_tax      float          NOT NULL,
      w_name     varchar(10)    NOT NULL,
      w_street_1 varchar(20)    NOT NULL,
      w_street_2 varchar(20)    NOT NULL,
      w_city     varchar(20)    NOT NULL,
      w_state    varchar(2)     NOT NULL,
      w_zip      varchar(9)     NOT NULL,
      PRIMARY KEY (w_id)
    );)";

static const std::string create_warehouse_index_sql =
  "CREATE INDEX warehouse_w_id ON warehouse (w_id)";

static const std::string create_item_sql =
  R"(CREATE TABLE item (
      i_id    int           NOT NULL,
      i_name  varchar(24)   NOT NULL,
      i_price float         NOT NULL,
      i_data  varchar(50)   NOT NULL,
      i_im_id int           NOT NULL,
      PRIMARY KEY (i_id)
    );)";

static const std::string create_item_index_sql =
  "CREATE INDEX item_i_id ON item (i_id)";

static const std::string create_stock_sql =
  R"(CREATE TABLE stock (
      s_w_id       int           NOT NULL,
      s_i_id       int           NOT NULL,
      s_quantity   int           NOT NULL,
      s_ytd        float         NOT NULL,
      s_order_cnt  int           NOT NULL,
      s_remote_cnt int           NOT NULL,
      s_data       varchar(50)   NOT NULL,
      s_dist_01    char(24)      NOT NULL,
      s_dist_02    char(24)      NOT NULL,
      s_dist_03    char(24)      NOT NULL,
      s_dist_04    char(24)      NOT NULL,
      s_dist_05    char(24)      NOT NULL,
      s_dist_06    char(24)      NOT NULL,
      s_dist_07    char(24)      NOT NULL,
      s_dist_08    char(24)      NOT NULL,
      s_dist_09    char(24)      NOT NULL,
      s_dist_10    char(24)      NOT NULL,
      FOREIGN KEY (s_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
      FOREIGN KEY (s_i_id) REFERENCES item (i_id) ON DELETE CASCADE,
      PRIMARY KEY (s_w_id, s_i_id)
    );)";

static const std::string create_stock_index_sql =
  "CREATE INDEX stock_s_w_id_s_i_id ON stock (s_w_id, s_i_id)";

static const std::string create_district_sql =
  R"(CREATE TABLE district (
      d_w_id      int            NOT NULL,
      d_id        int            NOT NULL,
      d_ytd       float          NOT NULL,
      d_tax       float          NOT NULL,
      d_next_o_id int            NOT NULL,
      d_name      varchar(10)    NOT NULL,
      d_street_1  varchar(20)    NOT NULL,
      d_street_2  varchar(20)    NOT NULL,
      d_city      varchar(20)    NOT NULL,
      d_state     char(2)        NOT NULL,
      d_zip       char(9)        NOT NULL,
      FOREIGN KEY (d_w_id) REFERENCES warehouse (w_id) ON DELETE CASCADE,
      PRIMARY KEY (d_w_id, d_id)
    );)";

static const std::string create_district_index_sql =
  "CREATE INDEX district_d_w_id_d_id ON district (d_w_id, d_id)";

static const std::string create_customer_sql =
  R"(CREATE TABLE customer (
      c_w_id         int            NOT NULL,
      c_d_id         int            NOT NULL,
      c_id           int            NOT NULL,
      c_discount     float          NOT NULL,
      c_credit       char(2)        NOT NULL,
      c_last         varchar(16)    NOT NULL,
      c_first        varchar(16)    NOT NULL,
      c_credit_lim   float          NOT NULL,
      c_balance      float          NOT NULL,
      c_ytd_payment  float          NOT NULL,
      c_payment_cnt  int            NOT NULL,
      c_delivery_cnt int            NOT NULL,
      c_street_1     varchar(20)    NOT NULL,
      c_street_2     varchar(20)    NOT NULL,
      c_city         varchar(20)    NOT NULL,
      c_state        char(2)        NOT NULL,
      c_zip          char(9)        NOT NULL,
      c_phone        char(16)       NOT NULL,
      c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,
      c_middle       char(2)        NOT NULL,
      c_data         varchar(500)   NOT NULL,
      FOREIGN KEY (c_w_id, c_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE,
      PRIMARY KEY (c_w_id, c_d_id, c_id)
    );)";

static const std::string create_customer_index_sql =
  "CREATE INDEX customer_c_w_id_c_d_id_c_id ON customer (c_w_id, c_d_id, c_id)";

static const std::string create_customer_name_index_sql =
  "CREATE INDEX customer_name ON customer (c_w_id, c_d_id, c_last, c_first)";

static const std::string create_history_sql =
  R"(CREATE TABLE history (
      h_c_id   int           NOT NULL,
      h_c_d_id int           NOT NULL,
      h_c_w_id int           NOT NULL,
      h_w_id   int           NOT NULL,
      h_d_id   int           NOT NULL,
      h_date   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP,
      h_amount float         NOT NULL,
      h_data   varchar(24)   NOT NULL,
      FOREIGN KEY (h_c_w_id, h_c_d_id, h_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
      FOREIGN KEY (h_w_id, h_d_id) REFERENCES district (d_w_id, d_id) ON DELETE CASCADE
    );)";

static const std::string create_history_index_sql =
  "CREATE INDEX history_index ON history (h_c_id, h_c_d_id, h_c_w_id, h_w_id, h_d_id, h_date)";

static const std::string create_oorder_sql =
  R"(CREATE TABLE oorder (
      o_w_id       int       NOT NULL,
      o_d_id       int       NOT NULL,
      o_id         int       NOT NULL,
      o_c_id       int       NOT NULL,
      o_carrier_id int       DEFAULT NULL,
      o_ol_cnt     int       NOT NULL,
      o_all_local  int       NOT NULL,
      o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (o_w_id, o_d_id, o_id),
      FOREIGN KEY (o_w_id, o_d_id, o_c_id) REFERENCES customer (c_w_id, c_d_id, c_id) ON DELETE CASCADE,
      UNIQUE (o_w_id, o_d_id, o_c_id, o_id)
    );)";

static const std::string create_oorder_index_sql =
  "CREATE INDEX oorder_o_w_id_o_d_id_o_id ON oorder (o_w_id, o_d_id, o_id)";

static const std::string create_oorder_customer_id_index_sql = 
  "CREATE INDEX oorder_c_id ON oorder (o_w_id, o_d_id, o_c_id, o_id)";

static const std::string create_new_order_sql =
  R"(CREATE TABLE new_order (
      no_w_id int NOT NULL,
      no_d_id int NOT NULL,
      no_o_id int NOT NULL,
      FOREIGN KEY (no_w_id, no_d_id, no_o_id) REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
      PRIMARY KEY (no_w_id, no_d_id, no_o_id)
    );)";

static const std::string create_new_order_index_sql =
  "CREATE INDEX new_order_no_w_id_no_d_id_no_o_id ON new_order (no_w_id, no_d_id, no_o_id)";

static const std::string create_order_line_sql =
  R"(CREATE TABLE order_line (
      ol_w_id        int           NOT NULL,
      ol_d_id        int           NOT NULL,
      ol_o_id        int           NOT NULL,
      ol_number      int           NOT NULL,
      ol_i_id        int           NOT NULL,
      ol_delivery_d  timestamp     NULL DEFAULT NULL,
      ol_amount      float         NOT NULL,
      ol_supply_w_id int           NOT NULL,
      ol_quantity    float         NOT NULL,
      ol_dist_info   char(24)      NOT NULL,
      FOREIGN KEY (ol_w_id, ol_d_id, ol_o_id) REFERENCES oorder (o_w_id, o_d_id, o_id) ON DELETE CASCADE,
      FOREIGN KEY (ol_supply_w_id, ol_i_id) REFERENCES stock (s_w_id, s_i_id) ON DELETE CASCADE,
      PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
    );)";

static const std::string create_order_line_index_sql =
  "CREATE INDEX order_line_ol_w_id_ol_d_id_ol_o_id_ol_number ON order_line (ol_w_id, ol_d_id, ol_o_id, ol_number)";

struct customer_key {
  int c_w_id;
  int c_d_id;
  int c_id;
};

struct customer_lastname_key {
  customer_lastname_key(int c_w_id, int c_d_id, const char c_last_[16], const char c_first_[16]) :
    c_w_id(c_w_id), c_d_id(c_d_id) {
      memcpy(c_last, c_last_, 16);
      memcpy(c_first, c_first_, 16);
  }
  int c_w_id;
  int c_d_id;
  char c_last[16];
  char c_first[16];
};

struct warehouse_key {
  int w_id;
};

struct district_key {
  int d_w_id;
  int d_id;
};

struct oorder_key {
  int o_w_id;
  int o_d_id;
  int o_id;
};

struct oorder_customer_id_key {
  int o_w_id;
  int o_d_id;
  int o_c_id;
  int o_id;
};

struct oorder_value {
  int o_w_id;
  int o_d_id;
  int o_id;
  int o_c_id;
  int o_carrier_id;
  int o_ol_cnt;
  int o_all_local;
  int64_t o_entry_d;
};

struct new_order_key {
  int no_w_id;
  int no_d_id;
  int no_o_id;
};

struct item_key {
  int i_id;
};

struct stock_key {
  int s_w_id;
  int s_i_id;
};

struct order_line_key {
  int ol_w_id;
  int ol_d_id;
  int ol_o_id;
  int ol_number;
};

struct order_line_value {
  order_line_value (int ol_w_id, int ol_d_id, int ol_o_id, int ol_number, int ol_i_id,
                    int64_t ol_delivery_d, float ol_amount, int ol_supply_w_id,
                    float ol_quantity, int ol_dist_info_offset, int ol_dist_info_size,
                    char ol_dist_info_[24]) :
    ol_w_id(ol_w_id), ol_d_id(ol_d_id), ol_o_id(ol_o_id), ol_number(ol_number),
    ol_i_id(ol_i_id), ol_delivery_d(ol_delivery_d), ol_amount(ol_amount),
    ol_supply_w_id(ol_supply_w_id), ol_quantity(ol_quantity),
    ol_dist_info_offset(ol_dist_info_offset), ol_dist_info_size(ol_dist_info_size) {
    memcpy(ol_dist_info, ol_dist_info_, 24);
  }
  int ol_w_id;
  int ol_d_id;
  int ol_o_id;
  int ol_number;
  int ol_i_id;
  int64_t ol_delivery_d;
  float ol_amount;
  int ol_supply_w_id;
  float ol_quantity;
  int ol_dist_info_offset;
  int ol_dist_info_size;
  char ol_dist_info[24];
};

struct history_value {
  history_value(int c_id, int c_d_id, int c_w_id, int w_id, int d_id, int64_t timestamp,
                float h_amount, int h_data_offset, int h_data_size, char *h_data_) : 
    c_id(c_id), c_d_id(c_d_id), c_w_id(c_w_id), w_id(w_id), d_id(d_id), timestamp(timestamp),
    h_amount(h_amount), h_data_offset(h_data_offset), h_data_size(h_data_size) {
    memcpy(h_data, h_data_, h_data_size);
  }
  int c_id;
  int c_d_id;
  int c_w_id;
  int w_id;
  int d_id;
  int64_t timestamp;
  float h_amount;
  int h_data_offset;
  int h_data_size;
  char h_data[24];
};

}  // namespace noname

