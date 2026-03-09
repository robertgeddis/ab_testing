/*------------------*/
/*   Create VIEW    */
/*------------------*/
drop view if exists analytics.vw_INTL_AB_Matrix;

create view analytics.vw_INTL_AB_Matrix as

-- <query>
select t.countrycode, t.testname, t.target, t.controlCellName, t.testCellName, t.startdate, t.enddate 
  ,case when def.testnameDisplay is null then t.controlCellName else def.testnameDisplay end as testnameDisplay
  ,case when def.controlCellnameDisplay is null then 'Control' else def.controlCellnameDisplay end as controlCellnameDisplay
  ,case when def.testCellnameDisplay is null then 'Test' else def.testCellnameDisplay end as testCellnameDisplay
  ,def.description
  ,case when datediff('hour', t.enddate, now()) > 12 then 'Ended' else 'Active' end as status
  ,case when def.winner = '' then 'Pending' else def.winner end as winner
from (
  /*------------------*/
  /*     vhpPages     */
  /*------------------*/
  -- Cells with naming convention
  select t.countrycode, t.testname, 'V' as target, t.controlCellName, t.testCellName
    ,case when t.startDateControl > t.startDateTest then t.startDateControl else t.startDateTest end as startdate
    ,case when t.endDateControl > t.endDateTest then t.endDateTest else t.endDateControl end as enddate
  from (
    select tcc.countrycode, tcc.testname, tcc.cellname as controlCellName, tct.cellname as testCellName
      ,min(tcc.assigneddate) startDateControl, max(tcc.assigneddate) as endDateControl
      ,min(tct.assigneddate) as startDateTest, max(tct.assigneddate) as endDateTest
    from intl.hive_testcell_assignment tcc
      join intl.hive_testcell_assignment tct on tct.countrycode = tcc.countrycode and tct.testname = tcc.testname and tct.cellname like 'V-%-T' 
        and tct.cellname = concat(substr(tcc.cellname, 0, length(tcc.cellname)-1),'-T')
        --and tct.cellname = replace(tcc.cellname, '-C', '-T') 
        and tct.year >= 2021 and tct.testname = 'vhpPages'
    where tcc.cellname like 'V-%-C' and tcc.year >= 2021 and tcc.testname = 'vhpPages'
    group by 1,2,3,4
  ) t
  
  UNION
  
  /*------------------*/
  /*  mobileVhpPages  */
  /*------------------*/
  -- Cells with naming convention
  select t.countrycode, t.testname, 'V' as target, t.controlCellName, t.testCellName
    ,case when t.startDateControl > t.startDateTest then t.startDateControl else t.startDateTest end as startdate
    ,case when t.endDateControl > t.endDateTest then t.endDateTest else t.endDateControl end as enddate
  from (
    select tcc.countrycode, tcc.testname, tcc.cellname as controlCellName, tct.cellname as testCellName
      ,min(tcc.assigneddate) startDateControl, max(tcc.assigneddate) as endDateControl
      ,min(tct.assigneddate) as startDateTest, max(tct.assigneddate) as endDateTest
    from intl.hive_testcell_assignment tcc
      join intl.hive_testcell_assignment tct on tct.countrycode = tcc.countrycode and tct.testname = tcc.testname and tct.cellname like 'V-%-T' 
        and tct.cellname = concat(substr(tcc.cellname, 0, length(tcc.cellname)-1),'-T')
        --and tct.cellname = replace(tcc.cellname, '-C', '-T') 
        and tct.year >= 2021 and tct.testname = 'mobileVhpPages'
    where tcc.cellname like 'V-%-C' and tcc.year >= 2021 and tcc.testname = 'mobileVhpPages'
    group by 1,2,3,4
  ) t

  UNION

  select 'au','mobileVhpPages','V','localized-vhp-ctrl','localized-vhp-test',to_timestamp('2020-11-02 17:09:13','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-01-19 09:01:34','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','mobileVhpPages','V','localized-vhp-ctrl-v2','localized-vhp-test-v2',to_timestamp('2020-12-02 13:35:24','YYYY-MM-DD hh:mi:ss'),to_timestamp('2020-12-18 09:25:41','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','mobileVhpPages','V','localized-vhp-ctrl-v4','localized-vhp-test-v4',to_timestamp('2021-01-06 12:45:21','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-01-18 13:31:40','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','mobileVhpPages','V','localized-vhp-ctrl','localized-vhp-test',to_timestamp('2020-11-17 11:45:05','YYYY-MM-DD hh:mi:ss'),to_timestamp('2020-12-02 13:36:37','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','mobileVhpPages','V','localized-vhp-ctrl-v3','localized-vhp-test-v3',to_timestamp('2020-12-18 08:06:59','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-01-06 12:55:05','YYYY-MM-DD hh:mi:ss') UNION 
  select 'au','vhpPages','V','localized-vhp-ctrl','localized-vhp-test',to_timestamp('2020-11-02 17:05:17','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-02-16 13:28:24','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','vhpPages','V','localized-vhp-ctrl','localized-vhp-test',to_timestamp('2020-11-18 10:52:11','YYYY-MM-DD hh:mi:ss'),to_timestamp('2020-12-02 13:34:46','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','vhpPages','V','localized-vhp-ctrl-v3','localized-vhp-test-v3',to_timestamp('2020-12-18 09:25:52','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-01-06 12:48:09','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','vhpPages','V','localized-vhp-ctrl-v2','localized-vhp-test-v2',to_timestamp('2020-12-02 13:39:20','YYYY-MM-DD hh:mi:ss'),to_timestamp('2020-12-18 09:49:23','YYYY-MM-DD hh:mi:ss') UNION 
  select 'ca','vhpPages','V','localized-vhp-ctrl-v4','localized-vhp-test-v4',to_timestamp('2021-01-06 12:40:16','YYYY-MM-DD hh:mi:ss'),to_timestamp('2021-01-18 13:31:40','YYYY-MM-DD hh:mi:ss')
  --pricing
  UNION SELECT 'de','pricing','S','S-DE-BER-35-70-140','S-DE-BER-39-79-159',to_timestamp('2016-09-16 15:06:42','YYYY-MM-DD hh:mi:ss'),to_timestamp('2016-12-01 07:50:25','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','S','S-DE-MUNC-35-70-140','S-DE-MUNC-44-84-164',to_timestamp('2015-07-23 12:21:32','YYYY-MM-DD hh:mi:ss'),to_timestamp('2015-11-02 14:12:42','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','S','S-DE-MUNC2-44-84-164','S-DE-MUNC2-49-94-194',to_timestamp('2015-10-02 13:57:19','YYYY-MM-DD hh:mi:ss'),to_timestamp('2015-12-22 10:53:31','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','S','S-DE-FRKF2-35-70-140','S-DE-FRKF2-39-79-159',to_timestamp('2015-08-25 08:31:59','YYYY-MM-DD hh:mi:ss'),to_timestamp('2016-02-11 10:45:39','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','S','S-DE-KLN-35-70-140','S-DE-KLN-39-79-159',to_timestamp('2016-09-16 15:28:48','YYYY-MM-DD hh:mi:ss'),to_timestamp('2017-01-25 08:23:30','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'uk','pricing','S','S-UK-LOW-35-70-140','S-UK-LOW-29-59-119',to_timestamp('2019-04-05 09:27:03','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-05-14 10:55:35','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'uk','pricing','S','S-UK-LOW-45-89-179-LDN','S-UK-LOW-29-59-119-LDN',to_timestamp('2019-04-05 09:27:03','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-05-14 10:55:35','YYYY-MM-DD hh:mi:ss')
  
  UNION SELECT 'de','pricing','P','P-DE-1812-15-30-45-BER','P-DE-1812-19-39-59-BER',to_timestamp('2018-12-11 09:39:31','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-03-07 08:25:43','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','P','P-DE-1812-15-30-45-MUN','P-DE-1812-19-39-59-MUN',to_timestamp('2018-12-11 10:08:45','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-03-07 10:31:21','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','P','P-DE-1812-15-30-45-FRF','P-DE-1812-19-39-59-FRF',to_timestamp('2018-12-11 10:09:00','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-05-12 23:03:44','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','P','P-DE-HAM-15-30-45','P-DE-HAM-19-39-59',to_timestamp('2019-01-22 13:32:54','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-03-07 10:09:48','YYYY-MM-DD hh:mi:ss')
  UNION SELECT 'de','pricing','P','P-DE-1812-15-30-45-KLN','P-DE-1812-19-39-59-KLN',to_timestamp('2018-12-11 10:08:31','YYYY-MM-DD hh:mi:ss'),to_timestamp('2019-05-13 07:27:39','YYYY-MM-DD hh:mi:ss')  
  
  UNION
  
  /*------------------*/
  /* AutoApp Unmasking */
  /*------------------*/
  select t.countrycode, t.testname, substr(controlCellName, 1, 1) as target, t.controlCellName, t.testCellName
    ,case when t.startDateControl > t.startDateTest then t.startDateControl else t.startDateTest end as startdate
    ,case when t.endDateControl > t.endDateTest then t.endDateTest else t.endDateControl end as enddate
  from (
    select tcc.countrycode, tcc.testname, tcc.cellname as controlCellName, tct.cellname as testCellName
      ,min(tcc.assigneddate) startDateControl, max(tcc.assigneddate) as endDateControl
      ,min(tct.assigneddate) as startDateTest, max(tct.assigneddate) as endDateTest
    from intl.hive_testcell_assignment tcc
      join intl.hive_testcell_assignment tct on tct.countrycode = tcc.countrycode and tct.testname = tcc.testname and tct.cellname like '%-T' 
        and tct.cellname = concat(substr(tcc.cellname, 0, length(tcc.cellname)-1),'-T')
        and tct.year >= 2022 and tct.testname = 'monolith-unmasked-auto-app-ab'
    where tcc.cellname like '%-C' and tcc.year >= 2022 and tcc.testname = 'monolith-unmasked-auto-app-ab' 
    group by 1,2,3,4
  ) t
 
  UNION
  
  /*------------------*/
  /*  Urgency Banner  */
  /*------------------*/
  select t.countrycode, t.testname, substr(controlCellName, 1, 1) as target, t.controlCellName, t.testCellName
    ,case when t.startDateControl > t.startDateTest then t.startDateControl else t.startDateTest end as startdate
    ,case when t.endDateControl > t.endDateTest then t.endDateTest else t.endDateControl end as enddate
  from (
    select tcc.countrycode, tcc.testname, tcc.cellname as controlCellName, tct.cellname as testCellName
      ,min(tcc.assigneddate) startDateControl, max(tcc.assigneddate) as endDateControl
      ,min(tct.assigneddate) as startDateTest, max(tct.assigneddate) as endDateTest
    from intl.hive_testcell_assignment tcc
      join intl.hive_testcell_assignment tct on tct.countrycode = tcc.countrycode and tct.testname = tcc.testname and tct.cellname like '%-T' 
        and tct.cellname = concat(substr(tcc.cellname, 0, length(tcc.cellname)-1),'-T')
        and tct.year >= 2022 and tct.testname = 'monolith-new-banner-headline-ab'
    where tcc.cellname like '%-C' and tcc.year >= 2022 and tcc.testname = 'monolith-new-banner-headline-ab' and date(tct.assigneddate)>='2022-05-09' 
    group by 1,2,3,4
  ) t
 
  UNION
  
  /*------------------*/
  /*     pricing      */
  /*------------------*/

  --Control cells
  select 
    c.countrycode, c.testname
    ,c.targetRole as target
    --,c.testIdentifier
    ,c.cellname as controlCellname
    ,t.cellname as testCellname
    ,case when c.firstassignment > t.firstassignment then c.firstassignment else t.firstassignment end as startdate
    ,case when c.lastassignment > t.lastassignment then t.lastassignment else c.lastassignment end as enddate
  from (
    select 
      countrycode, cellname, testname
      ,substring(cellname, 1, 1) as targetrole
      ,substring(cellname, 3, 2) as targetCountry
      ,substring(cellname, 6, instr(cellname, '-', 1, 3)-6) as testIdentifier
      ,substring(cellname, instr(cellname, '-', -1, 1) +1, length(cellname) - instr(cellname, '-', -1, 1) +1) as testVariation
      ,min(tc.assigneddate) as firstassignment, max(tc.assigneddate) as lastassignment
    from intl.hive_testcell_assignment tc
    where tc.year >= 2021 and memberid is not null and tc.testname = 'pricing' 
      and (cellname like ('S%') or cellname like ('P%')) 
      --and countrycode = 'de'
      and substring(cellname, instr(cellname, '-', -1, 1) +1, length(cellname) - instr(cellname, '-', -1, 1) +1) like ('%C%')
    group by 1,2,3,4,5,6,7
  ) c 
    left join (
      -- test cells
      select 
        countrycode, cellname, testname
        ,substring(cellname, 1, 1) as targetrole
        ,substring(cellname, 3, 2) as targetCountry
        ,substring(cellname, 6, instr(cellname, '-', 1, 3)-6) as testIdentifier
        ,substring(cellname, instr(cellname, '-', -1, 1) +1, length(cellname) - instr(cellname, '-', -1, 1) +1) as testVariation
        ,min(tc.assigneddate) as firstassignment, max(tc.assigneddate) as lastassignment
      from intl.hive_testcell_assignment tc
      where tc.year >= 2021 and tc.memberid is not null 
        and tc.testname = 'pricing' 
        and (cellname like ('S%') or cellname like ('P%'))
        --and countrycode = 'de'
        and substring(cellname, instr(cellname, '-', -1, 1) +1, length(cellname) - instr(cellname, '-', -1, 1) +1) like ('%T%')
      group by 1,2,3,4,5,6,7
    ) t on t.countrycode = c.countrycode and t.testname = c.testname and t.targetCountry = c.targetCountry 
       and t.targetrole = c.targetrole and t.testIdentifier = c.testIdentifier and length(c.testVariation) = length(t.testVariation)
  -- remove cells where first and last assignment is on the same day
  where 
    date(c.firstassignment) <> date(c.lastassignment) and date(t.firstassignment) <> date(t.lastassignment)
    and date(c.firstassignment) > '2021-01-01'
) t left join analytics_prod.INTL_AB_Matrix_Definitions def on t.countrycode = def.countrycode and t.testname = def.testname and t.controlCellName = def.controlCellname
  where datediff('hour', t.startdate, t.enddate) > 5 -- and year(t.startdate) >= 2021
-- </query>
;
grant SELECT on analytics.vw_INTL_AB_Matrix to reporting_ro, analytics_team;


select * from analytics.vw_INTL_AB_Matrix 
