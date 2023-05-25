drop table if exists sales
;
drop table if exists sale_journals
;
drop table if exists journal_closings
;
drop table if exists shop_journal_closings
;
drop table if exists sale_journal_processes
;
drop table if exists process_results
;
drop table if exists sale_journals__key_sales
;
create table sales (
    sale_id serial8 not null primary key,
    sale_date date not null,
    shop_id int8 not null,
    price int8,
    created_at timestamp default current_timestamp,     
    updated_at timestamp default current_timestamp     
)
;
create table sale_journals (
    sale_journal_id serial8 not null primary key,
    journal_closing_date date not null,
    sale_date date not null,
    shop_id int8 not null,
    price int8,
    created_at timestamp default current_timestamp
)
;
create table journal_closings (
    journal_closing_id serial8 not null primary key,
    journal_closing_date date not null,
    created_at timestamp default current_timestamp
)
;
create table shop_journal_closings (
    shop_journal_closing_id serial8 not null primary key,
    shop_id int8 not null,
    journal_closing_date date not null,
    created_at timestamp default current_timestamp
)
;
CREATE UNIQUE INDEX i1_shop_journal_closings ON shop_journal_closings (shop_id, journal_closing_date);
;
--test data
INSERT INTO sales (sale_date, shop_id, price, created_at)
select
    sale_date,
    shop_id,
    price,
    sale_date::timestamp + '-1 day' + random() * interval '3 days' as created_at
from
    (
        select
            date_trunc('day', CURRENT_DATE::timestamp + '1 day' - random() * interval '2 month')::date AS sale_date,
            floor(random() * 3) + 1 AS shop_id,
            floor(random() * 10000) + 1 AS price
        --    CURRENT_DATE::timestamp + '1 day' - random() * interval '2 month' as created_at
        FROM
            generate_series(1, 10000)
    ) d
;

insert into journal_closings (
    journal_closing_date,
    created_at
)
select
    journal_closing_date,
    (sale_date::timestamp + interval '19 hours')::timestamp + random() * interval '6 hours' as created_at
from
    (
        select distinct
            sale_date as journal_closing_date,
            sale_date
        from
            sales
    ) d
order by 
    journal_closing_date
;
insert into shop_journal_closings (
    journal_closing_date,
    shop_id,
    created_at
)
select
    journal_closing_date,
    shop_id,
    (sale_date::timestamp + interval '17 hours')::timestamp + random() * interval '1 day' as created_at
from
    (
        select distinct
            sale_date as journal_closing_date,
            sale_date,
            shop_id        
        from
            sales
    ) d
order by 
    sale_date,
    shop_id
;
select * from journal_closings
;
select * from shop_journal_closings
;