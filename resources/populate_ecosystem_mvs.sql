insert into voters_monthly_count
select toStartOfMonth(created_at) as month_start, uniqState(voter) as voters_count
from votes_raw group by month_start;

insert into voters_start
select voter,
       minState(created_at) as start_date
from votes_raw group by voter;