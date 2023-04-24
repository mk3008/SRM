drop table if exists sales
;
drop table if exists sale_journals
;
create table sales (
    sale_id serial8 not null primary key,
    sale_date date not null,
    price int8,
    remarks text,
    created_at timestamp default current_timestamp,     
    updated_at timestamp default current_timestamp     
)
;
create table sale_journals (
    sale_journal_id serial8 not null primary key,
    sale_date date not null,
    price int8,
    remarks text,
    created_at timestamp default current_timestamp
)
;
--test data
INSERT INTO sales (sale_date, price, remarks)
SELECT
  date_trunc('day', (CURRENT_DATE - random() * interval '365 days')),
  floor(random() * 100000),
  concat('remarks_', floor(random() * 10))
FROM generate_series(1, 10000);