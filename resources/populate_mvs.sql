insert into dao_voters_count
select dao_id, toStartOfMonth(created_at) as month_start, uniqExactState(voter) as voters_count
from votes_raw group by dao_id, month_start;

insert into dao_voters_start
select dao_id, voter,
       minState(created_at) as start_date
from votes_raw group by dao_id, voter;