/* to start out looking at data quality, i checked for missing values in the users, transactions, and products tables. 
i also checked for dupliate primary keys. */


/* in the users table, i noticed missing birth dates, null states, null genders, and a significant amount of records (1/3) 
missing a language. i didn't find any of the fields in the users table hard to understand, they all felt pretty straighforward. 
one issue i did run into is that `language` is a keyword in bigquery sql (what i'm using) and so i had to alias the table name 
and use that when queryin the `language` column to avoid run issues. */
select
    sum(if(u.id is null, 1, 0)) as ct_null_ids
    , sum(if(u.created_date is null, 1, 0)) as ct_null_created_date
    , sum(if(u.birth_date is null, 1, 0)) as ct_null_birth_date
    , sum(if(u.state is null, 1, 0)) as ct_null_state
    , sum(if(u.language is null, 1, 0)) as ct_null_language
    , sum(if(u.gender is null, 1, 0)) as ct_null_gender
    , count(*) as ct_rows
from `dgonyo-fetch-takehome.fetch_takehome.users` as u


;


/* i also double checked to make sure there were no duplicate ids (which there were not!) */
select
    u.id
    , count(*) as ct_dupes
from `dgonyo-fetch-takehome.fetch_takehome.users` as u
group by 1
having ct_dupes > 1


;


/* on the transactions table, i wouldn't expect to have any null values. whereas on `users`, it appeared to be largely 
self-described data from app users,`transactions` should be complete and comprehensive. there were 5,762 null barcodes and 
12,500 null sale values. to me, this means we have incomplete data in some way. maybe an upstream pipeline failed to run overnight, 
which means our barcode or sale table (that pulls into `transactions`) is out of date. none of the data is challenging to understand,
it's all pretty straightforward. */
select
    sum(if(t.receipt_id is null, 1, 0)) as ct_null_receipt_ids
    , sum(if(t.purchase_date is null, 1, 0)) as ct_null_purchase_date
    , sum(if(t.scan_date is null, 1, 0)) as ct_null_scan_date
    , sum(if(t.store_name is null, 1, 0)) as ct_null_store_name
    , sum(if(t.user_id is null, 1, 0)) as ct_null_user_id
    , sum(if(t.barcode is null, 1, 0)) as ct_null_barcode
    , sum(if(t.quantity is null, 1, 0)) as ct_null_quantity
    , sum(if(t.sale is null, 1, 0)) as ct_null_sale
from `dgonyo-fetch-takehome.fetch_takehome.transactions` as t


;


/* i also checked for duplicate receipt_ids on the `transactions` table, and was surprised at first to see so many duplicate 
records. my initial thought was 'oh this makes sense if they have multiple transactions on one receipt', but upon closer inspection, 
it appears that these dupes are the result of data quality issues, not expected duplicates. for example, receipt_id 
'0000d256-4041-4a3e-adc4-5623fb6e0c99' appears in the table twice. has the same value for all columns except for the 'sale' column 
which is null for one row and 1.54 for the other. looking at receipt_id 'fffe8012-7dcf-4d84-b6c6-feaacab5074a', all columns are the 
same except for 'quanitity', where one row is 'zero' and the other is 2.00. i'm not sure what the cause of this data quality issue 
is, but it needs to be resolved to remove these duplicate rows. */
with dupes as (
    select
        t.receipt_id
        , count(*) as ct_dupes
    from `dgonyo-fetch-takehome.fetch_takehome.transactions` as t
    group by 1
    having ct_dupes > 1
)

select
    d.*
    , t.* except(receipt_id)
from dupes as d
left join `dgonyo-fetch-takehome.fetch_takehome.transactions` as t using (receipt_id)
order by 1


;


/* the `products` table is pretty messy! the vast majority of records have at least one category (though not 100% -- 111 are 
missing a category). with that though, a huge amount are missing manufacturers and brands, and a small but not insignificant 
number are missing primary keys (barcodes). while it makese sense, it could be helpful to have better column naming conventions 
for the categories, or just have one 'category' column with an array of categories, rather than multiple category columns with 
mostly null rows. */
select
    sum(if(p.category_1 is null, 1, 0)) as ct_null_category_1
    , sum(if(p.category_2 is null, 1, 0)) as ct_null_category_2
    , sum(if(p.category_3 is null, 1, 0)) as ct_null_category_3
    , sum(if(p.category_4 is null, 1, 0)) as ct_null_category_4
    , sum(if(
        p.category_1 is null 
        and p.category_2 is null 
        and p.category_3 is null 
        and p.category_4 is null
    , 1, 0)) as ct_null_all_categories
    , sum(if(p.manufacturer is null, 1, 0)) as ct_null_manufacturer
    , sum(if(p.brand is null, 1, 0)) as ct_null_brand
    , sum(if(p.barcode is null, 1, 0)) as ct_null_barcode
from `dgonyo-fetch-takehome.fetch_takehome.products` as p


;


/* 185 barcodes appear more than once. i looked into them and a there's a few that just apear as duplicates because the table 
wasn't distincted -- for example, barcode 400510 appears twice in the table, but has the exact same values in the table. if you 
`select distinct`, this row disappears. looking at barcode 404310, it appears twice in the `products` table because it has two 
differen manufacturers and brands -- one row being placeholder data and the other being seemingly accurate inforamtion. */
with dupes as (
    select
        p.barcode
        , count(*) as ct_dupes
    from `dgonyo-fetch-takehome.fetch_takehome.products` as p
    group by 1
    having ct_dupes > 1
)

select distinct
    d.*
    , p.* except(barcode)
from dupes as d
left join `dgonyo-fetch-takehome.fetch_takehome.products` as p using (barcode)
where
    d.barcode is not null -- we know there are 4,025 rows with a null barcode, we want to exclude those from this list of dupes
order by 1











