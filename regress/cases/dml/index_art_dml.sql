DROP INDEX IF EXISTS idx_art_accounts_account_no;
DROP TABLE IF EXISTS art_accounts;

CREATE TABLE art_accounts (
    row_id INT PRIMARY KEY,
    account_no INT,
    balance INT
);

INSERT INTO art_accounts VALUES
    (1, 100, 500),
    (2, 200, 600),
    (3, 300, 700);

CREATE INDEX idx_art_accounts_account_no ON art_accounts (account_no);

INSERT INTO art_accounts VALUES
    (4, 400, 800),
    (5, 500, 900);

UPDATE art_accounts
SET account_no = 330, balance = 730
WHERE row_id = 3;

DELETE FROM art_accounts
WHERE row_id = 2;

SELECT row_id, balance
FROM art_accounts
WHERE account_no = 330
ORDER BY row_id;

SELECT COUNT(*) AS deleted_old_key
FROM art_accounts
WHERE account_no = 300;

SELECT row_id
FROM art_accounts
WHERE account_no BETWEEN 330 AND 500
ORDER BY row_id;

SELECT index_name, entry_count
FROM paro_indexes()
WHERE table_name = 'art_accounts'
ORDER BY index_name;

DROP TABLE art_accounts;
