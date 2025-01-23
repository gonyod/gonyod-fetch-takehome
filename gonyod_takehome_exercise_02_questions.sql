------------------------------------------------------------------------------
--- What are the top 5 brands by receipts scanned among users 21 and over? ---
------------------------------------------------------------------------------

/* when doing this exerceise, i noticed that the latest DOB is in 1997, but still included the DOB range as a precaution 
in case new data flows through. */
select
    p.brand
    , count(distinct t.receipt_id) as ct_receipts
from `dgonyo-fetch-takehome.fetch_takehome.products` as p
left join `dgonyo-fetch-takehome.fetch_takehome.transactions` as t on p.barcode = t.barcode
left join `dgonyo-fetch-takehome.fetch_takehome.users` as u on t.user_id = u.id
where
    -- gets all users with a DOB prior to 21 years ago today
    date(u.birth_date) <= date_sub(current_date('America/New_York'), interval 21 year)
    -- remove any rows with a null brand
    and p.brand is not null
group by 1
order by 2 desc
limit 5


;


----------------------------------------------------------------------------------------
--- What is the percentage of sales in the Health & Wellness category by generation? ---
----------------------------------------------------------------------------------------

/* i interpreted this question to be: broken down by generation, what percent of total sales are health & wellness sales? */

with base as (
    select
        case
            when u.birth_date between '1928-01-01' and '1945-12-31' then '01_silent_generation'
            when u.birth_date between '1946-01-01' and '1964-12-31' then '02_boomers'
            when u.birth_date between '1965-01-01' and '1980-12-31' then '03_gen_x'
            when u.birth_date between '1981-01-01' and '1996-12-31' then '04_millennials'
            when u.birth_date between '1997-01-01' and '2012-12-31' then '05_gen_z'
            when u.birth_date is null then '99_no_dob'
            else null
        end as generation
        /* it's not defined if these are nested categories or subcategories, so including all of them where 
           category = 'Health & Wellness' as a precaution */
        , sum(if(
            p.category_1 = 'Health & Wellness'
            or p.category_2 = 'Health & Wellness'
            or p.category_3 = 'Health & Wellness'
            or p.category_4 = 'Health & Wellness'
        , t.sale, null)) as health_and_wellness_sales
        , sum(t.sale) as total_sales
    from `dgonyo-fetch-takehome.fetch_takehome.products` as p
    left join `dgonyo-fetch-takehome.fetch_takehome.transactions` as t on p.barcode = cast(t.barcode as int64) /* CHANGE ME */
    left join `dgonyo-fetch-takehome.fetch_takehome.users` as u on t.user_id = u.id
    group by 1
)

select
    base.generation
    /* divides health_and_wellness_sales by total_sales, multiplies by 100 to get %, and rounds and makes nulls 0 */
    , ifnull(round(safe_multiply(safe_divide(base.health_and_wellness_sales, base.total_sales),100),2),0) as percent_health_and_wellness_sales
from base
order by 1


;


------------------------------------
--- Who are Fetch’s power users? ---
------------------------------------

/* i would define 'power users' as the top 25 users by # of receipts */

with do_count as (
    select
        u.id as user_id
        , count(distinct t.receipt_id) as ct_receipts
    from `dgonyo-fetch-takehome.fetch_takehome.users` as u
    left join `dgonyo-fetch-takehome.fetch_takehome.transactions` as t on u.id = t.user_id
    group by 1
)

select
    *    
    /* i was initially going to do a dense_rank() here to ensure that when there are ties, both users would count as a 
    power user. however, this data ranges from 0-3  receipts per user_id, which doesn't line up with what i'd expect 
    from Fetch power users. to make sure i'm not counting every user as a power user, i'm using a row_number() window 
    function instead, which splits the tie. */
    , row_number() over(order by ct_receipts desc) as ranking
from do_count
qualify ranking <= 25