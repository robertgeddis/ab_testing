select t.countrycode, t.locale, t.testname, t.cellname, t.startdate, t.enddate, t.description, t.testnameDisplay, t.status, t.winner, t.cellnamedisplay
   ,t.date, t.member_type, t.vertical, t.channel, t.payment_type, t.device, t.upgrade_duration, t.upgrade_type, t.promotionDiscount
   ,t.signups, t.upgrades
   ,sum(ltv.amount_net_usd)/count(distinct ltv.memberid) as ltv_per_member
   ,(sum(ltv.amount_net_usd)/count(distinct ltv.memberid))*t.upgrades as ltv_per_cohort
   --,count(distinct ltv.memberid) as ltv_denominator
from (
  select
    ab.countrycode, tc.locale, ab.testname, tc.cellname, ab.startdate, ab.enddate, ab.description, ab.testnameDisplay, ab.status, ab.winner
    ,case when tc.cellname = ab.controlcellname then ab.controlCellnameDisplay else ab.testCellnameDisplay end as cellnamedisplay
    ,date(tc.datecreated) as date
    /*,case 
      when tc.device = 'smartphone' then 'Mobile'
      when tc.device = '' or tc.device is null then 'Mobile' 
      else initcap(lower(tc.device))
    end as device*/
    ,case 
      when m.device = 'smartphone' then 'Mobile'
      when m.device = '' or m.device is null then 'Mobile' 
      else initcap(lower(m.device))
    end as device
    ,initcap(m.role) as member_type
    ,case 
      when m.vertical = 'homeCare' then 'Housekeeping' 
      when m.role is not null and (m.vertical IS NULL or m.vertical = '') then 'Childcare' 
      else initcap(lower(m.vertical))
    end as vertical
    ,case 
      --when m.role is null then null
      when lower(m.campaign) = 'seo' then 'SEO'
      when (lower(m.site) like 'careus%' or lower(m.campaign) = 'direct') then 'Direct'
      when (lower(m.audience) <> 'provider' and lower(m.campaign) = 'sem') then 'SEM'
      when (lower(m.campaign) = 'online' and lower(m.site) = 'facebook') then 'Facebook'
      when lower(m.campaign) in ('online','display','affiliate') then 'Online/Aff/Disp'
      when (lower(m.campaign) in ('email','onsite','','wps','b2b')) then 'Other'
      when m.campaign IS NULL then 'Other'
      when lower(m.audience) = 'provider' then 'Other'
      else 'Other'
    end as channel  
    ,cc.payment_type
    --sub.type is incorrectly populated, take the member data to determine the type instead
    --,sub.type as upgrade_type
    ,case 
      when date(sub.subscriptionDateCreated) = date(dateProfileComplete) then 'day1Upgrade'
      when date(m.dateFirstPremiumSignup) = date(sub.subscriptionDateCreated) and date(sub.subscriptionDateCreated) != date(dateProfileComplete) then 'dayNUpgrade'
      when date(m.dateFirstPremiumSignup) != date(sub.subscriptionDateCreated) then 'reUpgrade'
    end as upgrade_type
    ,sub.pricePlanDurationInMonths as upgrade_duration
    ,sub.promotionDiscount
    ,count(distinct m.memberid) as signups
    ,count(distinct sub.memberid) as upgrades
  from analytics.vw_INTL_AB_Matrix ab
    join intl.hive_event tc on tc.countrycode = ab.countrycode and tc.testname = ab.testname and tc.cellname in (ab.controlcellname, ab.testcellname)
      and tc.datecreated between ab.startdate and ab.enddate 
      and tc.name = 'TestCellAssignment' and date(tc.datecreated) <> current_date() and tc.year >= 2015
    join intl.hive_member m on m.countrycode = tc.countrycode and m.memberid = tc.memberid and m.isinternalaccount is not true and date(tc.datecreated) = date(m.datemembersignup)
      and m.closedforfraud is not true
    left join intl.transaction t on t.member_id = m.memberid and t.country_code = m.countrycode
      and t.type in ('PriorAuthCapture','AuthAndCapture') and t.status = 'SUCCESS' and t.amount > 0 
      and t.date_created between ab.startdate and ab.enddate
    left join intl.hive_subscription_plan sub on sub.memberid = t.member_id and sub.subscriptionid = t.subscription_plan_id and sub.countrycode = t.country_code
      and sub.subscriptiondatecreated between ab.startdate and ab.enddate
    left join intl.credit_card cc on cc.id = t.credit_card_id and cc.country_code = t.country_code and cc.member_id = t.member_id
  where ab.testname in ('pricing')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
) t 

  left join analytics_prod.INTL_LTV_Raw ltv on 
    ltv.countrycode = t.countrycode
    and ltv.device = t.device
    and ltv.member_type = t.member_type
    and ltv.vertical = t.vertical
    and ltv.channel = t.channel
    and ltv.payment_type = t.payment_type
    --Upgrade type (day1, nth, reup) should not be taken into account when calculating LTV on a member level
    --and ltv.upgrade_type = t.upgrade_type
    --and ltv.upgrade_duration = t.upgrade_duration
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
