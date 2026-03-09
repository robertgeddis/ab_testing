select distinct
  date(tc.datecreated) as assignment_date,
  upper(tc.countrycode) as countrycode,
  case when substring(tc.cellname, instr(tc.cellname, '-', -1, 1) +1, length(tc.cellname) - instr(tc.cellname, '-', -1, 1) +1) like ('%C%') then 'Control' else 'Test' end as cohort,
  case when lower(m.device) = 'smartphone' then 'Mobile' when (m.device = '' or m.device is null) then 'Mobile' else initcap(lower(m.device)) end as device,
  case when m.vertical = 'homeCare' then 'Housekeeping' when (m.vertical is null or m.vertical = '') then 'Childcare' else initcap(lower(m.vertical)) end as vertical,
  count(distinct tc.memberid) as assigned,
  count(distinct s.subscriptionId) as upgraded,
  s.pricePlanDurationInMonths as upgrade_duration,
  count(distinct case when date(s.subscriptionDateCreated) = date(m.dateProfileComplete) then s.subscriptionId end) as Day1s,
  count(distinct case when date(m.dateFirstPremiumSignup) = date(s.subscriptionDateCreated) and date(s.subscriptionDateCreated) != date(m.dateProfileComplete) then s.subscriptionId end) as Nths,
  count(distinct case when date(m.dateFirstPremiumSignup) != date(s.subscriptionDateCreated) then s.subscriptionId end) as Reupgrades,
  count(distinct pv.profileid) as profiles_viewed,
  count(distinct owner_id) as members_with_messages,
  count(distinct messages) as messages,
  count(distinct case when members_in_thread > 1 then messages end) as replies

from intl.hive_event tc

    join intl.hive_member m                     on m.countrycode = tc.countrycode and m.memberid = tc.memberid 
                                                and m.isinternalaccount is not true and m.closedforfraud is not true 
                                                and date(m.datemembersignup) >= '2024-08-01' and date(m.datemembersignup) < date(current_date) and lower(m.role) = 'seeker' 
                                             
    left join intl.transaction t               on t.member_id = m.memberid and t.country_code = m.countrycode
                                               and t.type in ('PriorAuthCapture','AuthAndCapture') and t.status = 'SUCCESS' and t.amount > 0 
                                               and t.date_created >= tc.datecreated and t.date_created >= '2024-08-01' and date(t.date_created) < date(current_date) 
                                             
    left join intl.hive_subscription_plan s    on s.memberid = t.member_id and s.subscriptionid = t.subscription_plan_id 
                                               and s.countrycode = t.country_code and s.subscriptiondatecreated >= tc.datecreated
                                               and s.subscriptiondatecreated >= '2024-08-01' and date(s.subscriptiondatecreated) < date(current_date)  
                                               
    left join intl.hive_event pv               on pv.memberid = tc.memberid and pv.countrycode = tc.countrycode and pv.datecreated > tc.datecreated  
                                               and pv.name = 'ProfileView' and date(pv.datecreated) >= '2024-08-01' and date(pv.datecreated) < date(current_date)     
                                               
    left join
          (select 
            date_created,
            country_code,
            owner_id,
            other_member_id,
            message_thread_id as messages,
            count(distinct members_in_thread) as members_in_thread
          from 
            (
              select
                msg.date_created,
                msg.owner_id,
                msg.other_member_id,
                msg.country_code, 
                msg.message_thread_id, 
                case
                  when msg.system_message_id = 1 then '(1) Bookmark'
                  when msg.system_message_id = 2 then '(2) Job Application'
                  when msg.system_message_id = 3 then '(3) No response'
                  when msg.system_message_id = 4 then '(4) Yes response'
                  when msg.system_message_id = 5 then '(5) Rating'
                  when msg.system_message_id = 6 then '(6) Say Hello'
                  when msg.system_message_id = 7 then '(7) Undetermined'
                  when msg.system_message_id = 8 then '(8) Custom text'
                  when msg.system_message_id = 9 then '(9) Question'
                  when msg.system_message_id = 10 then '(10) Quote Accepted'
                  when msg.system_message_id = 11 then '(11) Quote Rejected'
                  when msg.system_message_id is null then '(0) Manual'
                else 'Unknown'  end as message_type,
                fr.owner_id as members_in_thread
              from intl.message msg
                join intl.message fr on fr.message_thread_id = msg.message_thread_id and fr.country_code = msg.country_code and fr.sent_received = 'RECEIVED' and fr.pending is not true and fr.status in ('ACTIVE','INACTIVE')
                join intl.hive_member m on m.memberid = msg.owner_id and m.countrycode = msg.country_code and m.isinternalaccount is not true and m.closedforfraud is not true 
              where date(msg.date_created) >= '2024-08-01'
                    and date(msg.date_created) < date(current_date)  
                    and msg.first_message is true) t 
             where message_type in ('(9) Question', '(0) Manual', 'Unknown')
             group by 1,2,3,4,5) msg on msg.owner_id = pv.memberid and msg.country_code = pv.countrycode and msg.date_created > pv.datecreated and msg.other_member_id = pv.providerid                                          
                                                                             
where tc.name = 'TestCellAssignment'
  and tc.testname = 'monolith-carerank-ab'
  and date(tc.datecreated) >= '2024-08-01'
  and date(tc.datecreated) < date(current_date)
  and tc.memberid not in ('16866555', '16869237')

group by 1,2,3,4,5,8
