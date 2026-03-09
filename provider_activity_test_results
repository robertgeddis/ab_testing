select 
  ab.countrycode, 
  ab.testname, 
  tc.cellname, 
  ab.startdate, 
  ab.enddate, 
  ab.description, 
  ab.testnameDisplay, 
  ab.status, 
  ab.winner,
  case when tc.cellname = ab.controlcellname then ab.controlCellnameDisplay else ab.testCellnameDisplay end as cellnamedisplay,
  date(tc.datecreated) as assigned_date,
  ddd.week_start_date,
  case 
    when tc.device = 'smartphone' then 'Mobile'
    when tc.device = '' or tc.device is null then 'Mobile' 
    else initcap(lower(tc.device))
  end as user_device,
  case 
    when m.vertical = 'homeCare' then 'Housekeeping' 
    when (m.vertical IS NULL or m.vertical = '') then 'Childcare' 
    else initcap(lower(m.vertical))
  end as vertical,
  
  -- Basics
  count(distinct tc.memberid) as basics,
  count(distinct case when et.membersearchstatus='Approved' and et.name='ProfileCreate' then et.memberid end) as basics_with_profile,
  count(distinct case when et.membersearchstatus='Approved' and et.name='ProfileCreate' then et.profileid end) as profiles,
  count(distinct case when et.membersearchstatus='Approved' and et.name='ProfileCreate' and et.currentpageurl like '%saveBiography%' then et.profileid end) as profiles_enroll,
  count(distinct case when et.membersearchstatus='Approved' and et.name='ProfileCreate' and et.currentpageurl like '%saveEditProfile%' then et.profileid end) as profiles_account,
  count(distinct case when et.entityType='VerticalProfile' and et.approvalaction='Rejected' and et.name='CSRApproval' then et.objectid end) as profiles_rejected,
  
  -- Upgrades
  count(distinct case when et.name='Upgrade' then et.memberid end) as upgrades,
  count(distinct d.id) as downgrades,
  count(distinct case when d.found_care=1 then d.id end) as match,
  
  -- Completeness
  count(distinct p.member_id) as basics_with_photo,
  count(distinct r.reviewed_member_id) as reviews,
  r.rating,
  count(distinct case when et.name='Verification' and et.action='complete' and et.membersearchstatus='Approved' and et.statusvalue='success' then et.memberid end) as verifications,
  count(distinct doc.member_id) as doc,
 
  -- Activity
  count(distinct case when et.name='Login' then et.rkey end) as logins, 
  count(distinct case when et.name='Edit' and et.itemType='VerticalProfile' then et.memberid end) as basics_edited_profile, 
  count(distinct case when et.name='Edit' and et.itemType='VerticalProfile' then et.profileid end) as profiles_edited,
  count(distinct case when et.name='Edit' and et.itemType='VerticalProfile' then et.rkey end) as profile_edits,
  count(distinct case when et.name='JobApplication' and et.membersearchstatus='Approved' and et.action is null and et.issystemgenerated is null then et.jobapplicationid end) as manual_apps,
  
  -- Competitiveness
  count(distinct pv.memberid) as seeker_profile_views,
  count(distinct msg.owner_id) as seeker_contacts
  

from intl.hive_event tc

  join analytics.vw_INTL_AB_Matrix ab on ab.countrycode = tc.countrycode 
                                      and tc.testname = ab.testname 
                                      and tc.cellname in (ab.controlcellname, ab.testcellname)
  
  join intl.hive_member m on m.countrycode = tc.countrycode 
                          and m.memberid = tc.memberid 
                          and m.IsInternalAccount is not true 
                          and m.closedforfraud is not true
                          and lower(m.role) = 'provider' 
                          and date(m.datemembersignup) = date(tc.datecreated) 
                          and m.datemembersignup between ab.startdate and ab.enddate
  
  join reporting.dw_d_date ddd on date(tc.datecreated) = ddd.date
  
  left join intl.hive_event et on et.countrycode = tc.countrycode 
                               and et.memberid = tc.memberid 
                               and et.datecreated > tc.datecreated
                               and date(et.datecreated)>='2022-03-03'
                               and et.year >= 2022  
                               and et.name in ('ProfileCreate','Edit','CSRApproval','Upgrade','Verification','Login','Photo','Message','JobApplication') 
                               
  left join intl.CLOSE_DOWNGRADE_DETAIL d on d.country_code = tc.countrycode 
                                          and d.member_id = tc.memberid 
                                          and d.action_date > tc.datecreated 
                                          and d.user_type='Member' 
                                          and d.action_performed='Downgrade' 
                                          and date(d.action_date)>='2022-03-03'
                                          
  left join intl.review r on r.country_code = tc.countrycode 
                          and r.reviewed_member_id = tc.memberid 
                          and r.date_created > tc.datecreated
                          and r.first_approval_date is not null
                          and date(r.date_created)>='2022-03-03' 
                          
  left join intl.document doc on doc.country_code = tc.countrycode
                              and doc.member_id = tc.memberid  
                              and doc.date_created > tc.datecreated 
                              and doc.first_approval_date is not null 
                              and doc.search_status='Approved'
                              and date(doc.date_created)>='2022-03-03'
                              
  left join intl.hive_event pv on pv.countrycode = tc.countrycode 
                               and pv.providerid = tc.memberid 
                               and pv.memberid<>pv.providerid
                               and pv.datecreated > tc.datecreated 
                               and pv.name='ProfileView' 
                               and date(pv.datecreated)>='2022-03-03'
                               and pv.year >= 2022
                               
  left join intl.message msg on msg.country_code = tc.countrycode 
                         and msg.other_member_id = tc.memberid
                         and msg.date_created > tc.datecreated
                         and msg.sent_received='RECEIVED' 
                         and date(msg.date_created)>='2022-03-03'       
                         
  left join intl.photo p on p.country_code = tc.countrycode 
                         and p.member_id = tc.memberid  
                         and p.when_uploaded > tc.datecreated
                         and p.is_primary is true 
                         and p.first_approval_date is not null
                         and date(p.when_uploaded)>='2022-03-03'                     
  
where tc.name='TestCellAssignment' 
      and tc.year>=2022 and tc.memberid is not null 
      and tc.testname='firebird-onboarding-great-start-page-ab' 
      and date(tc.datecreated)<>current_date
      
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,26
