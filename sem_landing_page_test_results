with spend_data as (
  select 
    s.country,
    date(s.date) as date,
    d.week_start_date,
    s.device_type as device,
    s.campaign_name,
    case
      when s.device_type = 'DESKTOP' then concat('c_', s.campaign_name)
      when s.device_type = 'MOBILE' then concat('m_', s.campaign_name)
      when s.device_type = 'TABLET' then concat('t_', s.campaign_name)
    end as campaign_name_join,
    case 
      when lower(s.campaign_name) like '%_cc_%' then 'Childcare'
      when lower(s.campaign_name) like '%_hk_%' then 'Housekeeping'
    end as vertical,
    case when lower(s.campaign_name) like '%_instapage%' then 'Test' else 'Control' end as test_cell,
    sum(s.impressions) as impressions, sum(s.clicks) as clicks, 
    sum(case when s.currency = 'EUR' then s.cost*fx.currency_rate 
             when s.currency = 'AUD' then s.cost*fx.currency_rate end) as cost
  from intl.DW_F_CAMPAIGN_SPEND_INTL s
    join reporting.dw_d_date d on d.date = date(s.date)
    join reporting.DW_CARE_FX_RATES fx on s.currency=fx.source_currency and fx.target_currency = 'USD' and fx.currency_rate_type = 'Current'
  where
    lower(s.campaign_type) = 'seeker'
    and lower(s.source) = 'google'
    and s.country in ('de','au')
    and date(s.date) >= '2022-05-17'
    and lower(s.campaign_name) in 
    ('s_de_hk_putzfrau_02_src','s_de_hk_putzfrau_03_src','s_de_hk_putzfrau_02_src_instapage_v2','s_de_hk_putzfrau_03_src_instapage_v2', --- for DE
    's_cc_nanny_02','s_cc_nanny_03','s_cc_nanny_02_test_instapage_v2','s_cc_nanny_03_test_instapage_v2')   --- for AU
  group by 1,2,3,4,5,6,7,8
), 

visit_data as (
  select 
    v.countrycode,
    date(v.startdate) as date,
    v.rxcampaignname as campaignname, 
    count(distinct v.visitorid) as visits,
    count(distinct en.visitorid) as reach_enrollment
  from intl.hive_visit v
  left join intl.hive_event en on en.countrycode = v.countrycode and en.visitorid = v.visitorid and date(en.datecreated) = date(v.startdate) and en.sessionid = v.sessionid
                               and en.name = 'PageView' and en.memberid is null and en.year = 2022 and en.countrycode in ('de','au') and date(en.datecreated) >= '2022-05-17'
                               and en.currentpageurl like '%join-now%'
  where lower(v.rxcampaign)='sem'
    and date(v.startdate) >= '2022-05-17'
    and v.countrycode in ('au','de')
    and (v.memberid is null or v.signup=true)
    and v.rxcampaignname in 
  (
  'c_S_DE_HK_putzfrau_02_SRC_Instapage_v2',
  'm_S_DE_HK_putzfrau_02_SRC_Instapage_v2',
  't_S_DE_HK_putzfrau_02_SRC_Instapage_v2',

  'c_S_DE_HK_putzfrau_03_SRC_Instapage_v2',
  'm_S_DE_HK_putzfrau_03_SRC_Instapage_v2',
  't_S_DE_HK_putzfrau_03_SRC_Instapage_v2',

  'c_S_CC_nanny_02_test_Instapage_v2',
  'm_S_CC_nanny_02_test_Instapage_v2',
  't_S_CC_nanny_02_test_Instapage_v2',

  'c_S_CC_nanny_03_test_Instapage_v2',
  'm_S_CC_nanny_03_test_Instapage_v2',
  't_S_CC_nanny_03_test_Instapage_v2',

  'c_S_DE_HK_putzfrau_02_SRC',
  'm_S_DE_HK_putzfrau_02_SRC',
  't_S_DE_HK_putzfrau_02_SRC',

  'c_S_DE_HK_putzfrau_03_SRC',
  'm_S_DE_HK_putzfrau_03_SRC',
  't_S_DE_HK_putzfrau_03_SRC',

  'c_S_CC_nanny_02',
  'm_S_CC_nanny_02',
  't_S_CC_nanny_02',

  'c_S_CC_nanny_03',
  'm_S_CC_nanny_03',
  't_S_CC_nanny_03'
  ) 
 group by 1,2,3
),

basic_data as (
  select
    countrycode,
    date(dateProfileComplete) as 'date',
    campaignname,
    count(distinct memberid) as 'SEM_basics'
  from intl.hive_member
  where
    date(dateProfileComplete) >= '2022-05-17'
    and lower(site) = 'google' 
    and role = 'seeker'
    and audience <> 'provider' 
    and lower(campaign) = 'sem'    
    and IsInternalAccount = 'false' 
    and date(dateProfileComplete) <> current_date 
    and countrycode in ('de','au')
    and (lower(campaignname) like '%s_de_hk_putzfrau_02_src%' or lower(campaignname) like '%s_de_hk_putzfrau_03_src%' or lower(campaignname) like '%s_cc_nanny_02%' or lower(campaignname) like '%s_cc_nanny_03%')
  group by 1,2,3
),

premium_data as (
  select
    m.countrycode,
    date(sp.subscriptionDateCreated) as date,
    m.campaignname,
    count(distinct sp.subscriptionId) as 'SEM_New_Premiums',
    count(distinct case when date(sp.subscriptionDateCreated) = date(m.datemembersignup) then sp.subscriptionId end) as 'SEM_New_Premiums_D1',
    count(distinct case when date(sp.subscriptionDateCreated) <> date(m.datemembersignup) then sp.subscriptionId end) as 'SEM_New_Premiums_Nth'
  from intl.transaction t
    join intl.hive_subscription_plan sp on sp.subscriptionId = t.subscription_plan_id and sp.countrycode = t.country_code
    join intl.hive_member m on t.member_id = m.memberid and t.country_code = m.countrycode
      and m.role = 'seeker' 
      and m.IsInternalAccount = 'false' 
      and lower(m.site) = 'google' 
      and m.audience <> 'provider' 
      and lower(campaign) = 'sem' 
      and m.countrycode in ('de','au')
      and (lower(m.campaignname) like '%s_de_hk_putzfrau_02_src%' or lower(m.campaignname) like '%s_de_hk_putzfrau_03_src%' or lower(m.campaignname) like '%s_cc_nanny_02%' or lower(m.campaignname) like '%s_cc_nanny_03%')
  where
    t.type in ('PriorAuthCapture','AuthAndCapture')
    and t.status = 'SUCCESS'
    and t.amount > 0
    and date(m.dateFirstPremiumSignup) = date(sp.subscriptionDateCreated)
    and date(sp.subscriptionDateCreated) <> current_date 
    and date(dateProfileComplete) >= '2022-05-17'
  group by 1,2,3
)

select s.country, s.date, s.week_start_date, s.device, s.vertical, s.campaign_name, s.test_cell, s.impressions, s.clicks, s.cost, campaign_name_join
  ,ifnull(v.visits, 0) as visits
  ,ifnull(v.reach_enrollment, 0) as reach_enrollment
  ,ifnull(b.SEM_basics, 0) as basics
  ,ifnull(p.SEM_New_Premiums, 0) as upgrades
  ,ifnull(p.SEM_New_Premiums_D1, 0) as upgradesD1
  ,ifnull(p.SEM_New_Premiums_Nth, 0) as upgradesNth
from spend_data s
  left join visit_data v on v.countrycode = s.country and v.date = s.date and lower(v.campaignname) = lower(s.campaign_name_join)
  left join basic_data b on b.countrycode = s.country and b.date = s.date and lower(b.campaignname) = lower(s.campaign_name_join)  
  left join premium_data p on p.countrycode = s.country and p.date = s.date and lower(p.campaignname) = lower(s.campaign_name_join) 
  
