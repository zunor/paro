// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0
//
// Derived from Databend (https://github.com/datafuselabs/databend),
// Copyright 2021 Datafuse Labs, also licensed under Apache License 2.0.

use crate::common::{write_statement_cases, write_statement_error_cases};

#[test]
fn test_acl_statements() {
    let cases = &[
        r#"show users"#,
        r#"show roles"#,
        r#"create role role1 comment='test';"#,
        r#"alter role role1 set comment='test';"#,
        r#"alter role role1 unset comment;"#,
        r#"ALTER USER u1 IDENTIFIED BY '123456';"#,
        r#"ALTER USER u1 WITH disabled = false;"#,
        r#"ALTER USER u1 WITH default_role = role1;"#,
        r#"ALTER USER u1 WITH DEFAULT_ROLE = role1, DISABLED=true, TENANTSETTING;"#,
        r#"ALTER USER u1 WITH SET NETWORK POLICY = 'policy1';"#,
        r#"ALTER USER u1 WITH UNSET NETWORK POLICY;"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH SET WORKLOAD GROUP='W1'"#,
        r#"ALTER USER u1 WITH SET WORKLOAD GROUP = 'W1';"#,
        r#"ALTER USER u1 WITH UNSET WORKLOAD GROUP;"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH DEFAULT_ROLE='role123', TENANTSETTING"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH SET NETWORK POLICY='policy1'"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH disabled=true"#,
        r#"create user 'test-e' identified by 'password';"#,
        r#"drop user if exists 'test-j';"#,
        r#"alter user 'test-e' identified by 'new-password';"#,
        r#"create role test"#,
        r#"create role 'test'"#,
        r#"create user `a'a` identified by '123'"#,
        r#"drop role if exists test"#,
        r#"drop role if exists 'test'"#,
        r#"GRANT CREATE, CREATE USER ON * TO 'test-grant';"#,
        r#"GRANT access connection, create connection ON *.*  TO 'test-grant';"#,
        r#"GRANT access connection on connection c1  TO 'test-grant';"#,
        r#"GRANT all on connection c1  TO 'test-grant';"#,
        r#"GRANT OWNERSHIP on connection c1  TO role r1;"#,
        r#"GRANT OWNERSHIP on masking policy m1  TO role r1;"#,
        r#"GRANT access sequence, create sequence ON *.*  TO 'test-grant';"#,
        r#"GRANT access sequence on sequence s1  TO 'test-grant';"#,
        r#"GRANT all on sequence s1  TO 'test-grant';"#,
        r#"GRANT OWNERSHIP on sequence s1  TO role r1;"#,
        r#"GRANT SELECT, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.* TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO USER 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO ROLE role1;"#,
        r#"GRANT ALL ON *.* TO 'test-grant';"#,
        r#"GRANT ALL ON *.* TO ROLE role2;"#,
        r#"GRANT ALL PRIVILEGES ON * TO 'test-grant';"#,
        r#"GRANT ALL PRIVILEGES ON * TO ROLE role3;"#,
        r#"GRANT ROLE test TO 'test-user';"#,
        r#"GRANT ROLE test TO USER 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"GRANT SELECT ON db01.* TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO ROLE role1"#,
        r#"GRANT SELECT ON db01.tb1 TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO ROLE role1;"#,
        r#"GRANT SELECT ON tb1 TO ROLE role1;"#,
        r#"GRANT ALL ON tb1 TO 'u1';"#,
        r#"GRANT CREATE MASKING POLICY ON *.* TO USER a;"#,
        r#"GRANT APPLY MASKING POLICY ON *.* TO USER a;"#,
        r#"GRANT APPLY ON MASKING POLICY ssn_mask TO ROLE human_resources;"#,
        r#"GRANT OWNERSHIP ON MASKING POLICY mask_phone TO ROLE role_mask_apply;"#,
        r#"SHOW GRANTS;"#,
        r#"REVOKE SELECT, CREATE ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE role1;"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE 'role1';"#,
        r#"drop role 'role1';"#,
        r#"GRANT ROLE test TO ROLE 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"REVOKE ALL ON tb1 FROM 'u1';"#,
        r#"GRANT all ON stage s1 TO a;"#,
        r#"GRANT read ON stage s1 TO a;"#,
        r#"GRANT write ON stage s1 TO a;"#,
        r#"REVOKE write ON stage s1 FROM a;"#,
        r#"GRANT all ON UDF a TO 'test-grant';"#,
        r#"GRANT usage ON UDF a TO 'test-grant';"#,
        r#"REVOKE usage ON UDF a FROM 'test-grant';"#,
        r#"REVOKE all ON UDF a FROM 'test-grant';"#,
        r#"GRANT all ON warehouse a TO role 'test-grant';"#,
        r#"GRANT usage ON warehouse a TO role 'test-grant';"#,
        r#"REVOKE usage ON warehouse a FROM role 'test-grant';"#,
        r#"REVOKE all ON warehouse a FROM role 'test-grant';"#,
        r#"CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********'END comment = 'this is a masking policy'"#,
        r#"DESC MASKING POLICY email_mask"#,
        r#"DROP MASKING POLICY IF EXISTS email_mask"#,
        r#"CREATE NETWORK POLICY mypolicy ALLOWED_IP_LIST=('192.168.10.0/24') BLOCKED_IP_LIST=('192.168.10.99') COMMENT='test'"#,
        r#"CREATE OR REPLACE NETWORK POLICY mypolicy ALLOWED_IP_LIST=('192.168.10.0/24') BLOCKED_IP_LIST=('192.168.10.99') COMMENT='test'"#,
        r#"ALTER NETWORK POLICY mypolicy SET ALLOWED_IP_LIST=('192.168.10.0/24','192.168.255.1') BLOCKED_IP_LIST=('192.168.1.99') COMMENT='test'"#,
        r#"create row access policy rap_it as (empl_id varchar) returns boolean ->
          case
              when 'it_admin' = current_role() then true
              else false
          end"#,
        r#"create row access policy if not exists rap_sales_manager_regions_1 as (sales_region varchar) returns boolean ->
            'sales_executive_role' = current_role()
              or exists (
                    select 1 from salesmanagerregions
                      where sales_manager = current_role()
                        and region = sales_region
        )"#,
        r#"DROP row access policy IF EXISTS r1"#,
        r#"desc row access policy r1"#,
        r#"GRANT CREATE ROW ACCESS POLICY ON *.* TO USER a;"#,
        r#"GRANT APPLY ROW ACCESS POLICY ON *.* TO USER a;"#,
        r#"GRANT APPLY ON ROW ACCESS POLICY ssn_mask TO ROLE 'human_resources'"#,
        r#"GRANT OWNERSHIP ON  ROW ACCESS POLICY mask_phone TO ROLE 'role_mask_apply'"#,
    ];

    write_statement_cases("acl.txt", cases);
}

#[test]
fn test_acl_statement_errors() {
    let cases = &[
        r#"create user 'test-e' identified bi 'password';"#,
        r#"create user 'test-e'@'localhost' identified by 'password';"#,
        r#"drop usar if exists 'test-j';"#,
        r#"alter user 'test-e' identifies by 'new-password';"#,
        r#"create role 'test'@'%';"#,
        r#"drop role 'test'@'%';"#,
        r#"create role `a"a`"#,
        r#"create role `a'a`"#,
        r#"create role `a\ba`"#,
        r#"create role `a\fa`"#,
        r#"drop role `a\fa`"#,
        r#"SHOW GRANT FOR ROLE 'role1';"#,
        r#"GRANT ROLE 'test' TO ROLE test-user;"#,
        r#"GRANT SELECT, ALL PRIVILEGES, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.c TO 'test-grant';"#,
        r#"GRANT select ON UDF a TO 'test-grant';"#,
        r#"REVOKE SELECT, CREATE, ALL PRIVILEGES ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT, CREATE ON * TO 'test-grant';"#,
        r#"GRANT OWNERSHIP, SELECT ON d20_0014.* TO ROLE 'd20_0015_owner';"#,
        r#"GRANT OWNERSHIP ON d20_0014.* TO USER A;"#,
        r#"REVOKE OWNERSHIP, SELECT ON d20_0014.* FROM ROLE 'd20_0015_owner';"#,
        r#"REVOKE OWNERSHIP ON d20_0014.* FROM USER A;"#,
        r#"REVOKE OWNERSHIP ON d20_0014.* FROM ROLE A;"#,
        r#"GRANT OWNERSHIP ON *.* TO ROLE 'd20_0015_owner';"#,
    ];

    write_statement_error_cases("acl-error.txt", cases);
}
