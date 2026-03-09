drop table if exists analytics_prod.INTL_AB_CRM_Promocodes;
CREATE TABLE analytics_prod.INTL_AB_CRM_Promocodes (
  promoCode VARCHAR(200)
);

INSERT INTO analytics_prod.INTL_AB_CRM_Promocodes VALUES ('15GOPREMIUM10');
INSERT INTO analytics_prod.INTL_AB_CRM_Promocodes VALUES ('20GOPREMIUM10');
INSERT INTO analytics_prod.INTL_AB_CRM_Promocodes VALUES ('M15GOPREMIUM10');
INSERT INTO analytics_prod.INTL_AB_CRM_Promocodes VALUES ('M20GOPREMIUM10');

GRANT ALL on analytics_prod.INTL_AB_CRM_Promocodes to reporting_ro, analytics_team WITH GRANT OPTION;
-- select * from analytics_prod.INTL_AB_CRM_Promocodes

drop table if exists analytics_prod.INTL_AB_CRM_Cell_Config;
CREATE TABLE analytics_prod.INTL_AB_CRM_Cell_Config (
  testId INT NOT NULL
  ,campaignName VARCHAR(500)
  ,memberidLastDigit INT NOT NULL
  ,variation VARCHAR(100) NOT NULL
);
--Provider Welcome Stream Test
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (1,'',0,'Control');
--Test - 19-20 Oct 2021
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch',1,'C. Test 20%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch',2,'B. Test 15%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium_Launch',3,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium_Launch',4,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch',5,'B. Test 15%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch',6,'B. Test 15%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch',7,'C. Test 20%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch',8,'C. Test 20%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch',9,'C. Test 20%');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (2,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch',0,'B. Test 15%');
-- Request a review
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',1,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',2,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',3,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',4,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',5,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',6,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',7,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',8,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',9,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (3,'',0,'C. Noticed');
-- Add a Photo
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',1,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',2,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',3,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',4,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',5,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',6,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',7,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',8,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',9,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (4,'',0,'C. Noticed');
-- Add a review DE
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',1,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',2,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',3,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',4,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',5,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',6,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',7,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',8,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',9,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (5,'',0,'C. Noticed');
-- Add a photo DE
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',1,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',2,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',3,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',4,'A. Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',5,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',6,'C. Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',7,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',8,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',9,'B. Contacted');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (6,'',0,'C. Noticed');

INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (7,'',0,'Control');

INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (8,'',0,'Control');

INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (9,'',0,'Control');

INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (10,'',0,'Control');

--Providers Near You Test 16.11.2022 (ADI-8998)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (11,'',0,'Control');

--Providers Near You / Welcome Stream Test 01.12.2022 (ADI-9066)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (12,'',0,'Control');

--Seeker Welcome Email Redesign 07.02.2023 (ADI-9558)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (13,'',0,'Control');

--Provider Welcome Email Redesign 07.02.2023 (ADI-9558)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (14,'',0,'Control');

--New Provider Welcome Email 31.03.2023 (ADI-9969)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (15,'',0,'Control');

--New Provider Toolkit Email 02.05.2023 (ADI-10152)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (16,'',0,'Control');

--New Seeker Promo Welcome Email 17.05.2023 (ADI-10344)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (17,'',0,'Control');

--Seeker Onboarding Promo Email (Day 2) Redesign 25.05.2023 (ADI-10501)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (18,'',0,'Control');

--Promo Code in Job App Trigger 25.05.2023 (ADI-10504)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (19,'',0,'Control');

--30% Promo Code in Abandoned Cart 13.06.2023 (ADI-10626)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (20,'',0,'Control');

--Fast20 Code in Seeker Welcome Email 15.06.2023 (ADI-10653)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',4,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',5,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (21,'',0,'Control');

--Promo in AutoApp Received 27.06.2023 (ADI-10750)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',1,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',2,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',4,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',5,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (22,'',0,'Test');

--Promo in Message Received 29.06.2023 (ADI-10758)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',1,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',2,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',4,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',5,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (23,'',0,'Test');

--FAST20 + US Copy in Seeker Welcome Email (ADI-10788)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',4,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',5,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (24,'',0,'Control');

--Providers Near You View CTA (ADI-10795)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (25,'',0,'Control');

--Jobs Near You SL (ADI-10840)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (26,'',0,'Control');

--Jobs Near You CTA: See Details (ADI-10932)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (27,'',0,'Control');

--Jobs Near You CTA: Be the First to Apply (ADI-11031)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (28,'',0,'Control');

--Abandoned Rate Card Day0 Exclusion: Seeker + Provider (ADI-11193)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (29,'',0,'Control');

--Abandoned Rate Card / Onboarding Promo Day 2 (ADI-11478)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (30,'',0,'Control');

--Merge Abandoned Rate Card with Welcome for High Intent Day1s (ADI-11820)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (31,'',0,'Control');

--Abandoned Rate Card: Reminder New Design + PNY feed (ADI-11821)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (32,'',0,'Control');

--Provider Profile Viewed - Improve Unsubs (ADI-11887)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (33,'',0,'Control');

--Abandoned Rate Card Dedicated Reupgrades Copy (ADI-11925)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (34,'',0,'Control');

--Valentines Day Promo DE (ASB-591)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',1,'Hold');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',3,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (35,'',0,'Hold');

--Spring Clean Promo DE/AT/UK/CA (ASB-12499)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (36,'',0,'Control');

--AutoApp Received SL (ADI-12516)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (37,'',0,'Control');

--Promo in PNY for Marketing Opt-Ins (ASB-1145)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (38,'',0,'Control');

--Promo and PNY in DE (ADI-12524)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (39,'',0,'Control');

--Job Posted Confirmation (ADI-12560)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',2,'Hold');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',4,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',5,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',6,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (40,'',0,'Hold');

--Robust Provider Profile Snippets in PNY (ASB-2091)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (41,'',0,'Control');

--My Carers LP in AutoApp Received Email (ASB-2141)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (42,'',0,'Control');

--PNY Robust Snippet DACH Markets (ADI-12608)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (43,'',0,'Control');

--PNY Robust Snippet EN Markets (ADI-12609)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (44,'',0,'Control');

--PNY Marketing Module Small Markets (ADI-12610)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (45,'',0,'Control');

--AutoApp Received Robust provider Snippet V1 (INTLADI-58)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (46,'',0,'Control');

--PNY Robust provider Snippet V3 (INTLADI-59)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (47,'',0,'Control');

--PNY Robust provider Snippet No Photo (INTLADI-60)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (48,'',0,'Control');

--AutoApp Received Robust Provider Snippet V2 (INTLADI-64)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (49,'',0,'Control');

--JobApp Robust Provider Snippet V1 (INTLADI-68)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (50,'',0,'Control');

--DE Promo and UGC Content (INTLADI-69)
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',1,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',2,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',3,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',4,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',5,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',6,'Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',7,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',8,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',9,'Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Cell_Config VALUES (51,'',0,'Control');

GRANT ALL on analytics_prod.INTL_AB_CRM_Cell_Config to reporting_ro, analytics_team WITH GRANT OPTION;

select * from analytics_prod.INTL_AB_CRM_Cell_Config order by 1,2,3,4;

drop table if exists analytics_prod.INTL_AB_CRM_Performance;
CREATE TABLE analytics_prod.INTL_AB_CRM_Performance (
  target VARCHAR(8) NOT NULL
  ,startdate TIMESTAMP NOT NULL
  ,enddate TIMESTAMP NOT NULL
  ,testid INT NOT NULL
  ,title VARCHAR (200)
  ,campaign_id INT
  ,campaign_name VARCHAR(500)
);
--Provider Welcome Stream Test
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'202109_Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_AppForJobs');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'202109_Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_CompleteYourProfile preview	202109_Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_CompleteYourProfile');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_UploadAPhoto_Claim');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_goPremium');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-09-30','2021-10-18',1,'Provider Welcome Stream Test',0,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_UnreadMessage');
--Test - 19-20 Oct 2021
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3007320,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3010669,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium20_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3007321,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3010668,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3007319,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-10-18','2021-10-25',2,'Test - 19-20 Oct 2021',3010670,'20211019_Email_Blast_B2C_Provider_Basic_AllV_Promo_GoPremium15_Launch');
-- Request a review
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-01','2021-11-08',3,'Request a Review',3096560,'20211102_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-01','2021-11-08',3,'Request a Review',3096497,'20211102_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-01','2021-11-08',3,'Request a Review',3096496,'20211102_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Contacted');
-- Add a Photo
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-07','2021-11-15',4,'Add a Photo',3106372,'20211108_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-07','2021-11-15',4,'Add a Photo',3106370,'20211108_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-07','2021-11-15',4,'Add a Photo',3106371,'20211108_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Contacted');
-- Request a review DE
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-22','2021-11-30',5,'Request a Review (DE)',3220316,'20211123_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-22','2021-11-30',5,'Request a Review (DE)',3220315,'20211123_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-22','2021-11-30',5,'Request a Review (DE)',3220314,'20211123_Email_Blast_B2C_Provider_Basic_AllV_Product_RequestReview_Contacted');
--Add a photo DE
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-29','2021-12-06',6,'Add a Photo (DE)',3262320,'20211130_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-29','2021-12-06',6,'Add a Photo (DE)',3262318,'20211130_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Noticed');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2021-11-29','2021-12-06',6,'Add a Photo (DE)',3262319,'20211130_Email_Blast_B2C_Provider_Basic_AllV_Product_AddaPhoto_Contacted');
--INTLBI-318
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-21','2022-02-28',7,'Premium Benefits (Launch)',3728784,'20220222_Email_Blast_B2C_Seeker_Basic_AllV_Promo_ADealtoHelpYouOut_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-21','2022-02-28',7,'Premium Benefits (Launch)',3728774,'20220222_Email_Blast_B2C_Seeker_Basic_AllV_Promo_ADealtoHelpYouOut_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-22','2022-03-01',8,'Premium Benefits (Rem)',3733352,'20220223_Email_Blast_B2C_Seeker_Basic_AllV_Promo_ADealtoHelpYouOut_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-22','2022-03-01',8,'Premium Benefits (Rem)',3733351,'20220223_Email_Blast_B2C_Seeker_Basic_AllV_Promo_ADealtoHelpYouOut_Rem');
--INTLBI-326
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-28','2022-03-07',9,'Go Local (Launch)',3758963,'20220301_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-28','2022-03-07',9,'Go Local (Launch)',3758969,'20220301_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-28','2022-03-07',9,'Go Local (Launch)',3758962,'20220301_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-02-28','2022-03-07',9,'Go Local (Launch)',3758970,'20220301_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-03-01','2022-03-08',10,'Go Local (Rem)',3759047,'20220302_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-03-01','2022-03-08',10,'Go Local (Rem)',3759053,'20220302_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-03-01','2022-03-08',10,'Go Local (Rem)',3759046,'20220302_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2022-03-01','2022-03-08',10,'Go Local (Rem)',3759052,'20220302_Email_Blast_B2C_Seeker_Basic_AllV_Promo_GoLocal_Rem');
--Providers Near You Test 16.11.2022 (ADI-8998)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-11-16','2022-11-24',11,'Providers Near You',5541773,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-11-16','2022-11-24',11,'Providers Near You',5541451,'Transactional-Email_ProviderNearYou');
--Providers Near You / Welcome Stream Test 01.12.2022 (ADI-9066)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-12-01','2023-01-13',12,'Providers Near You / Welcome Stream',1598851,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-12-01','2023-01-13',12,'Providers Near You / Welcome Stream',5541451,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-12-01','2023-01-13',12,'Providers Near You / Welcome Stream',2044650,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2022-12-01','2023-01-13',12,'Providers Near You / Welcome Stream',5652114,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Test_Promo');

--Seeker Welcome Email Redesign 07.02.2023 (ADI-9558)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',1338991,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',1329089,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',1328663,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',6109596,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',6109879,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-02-07','2023-03-01',13,'Seeker Welcome Email Redesign',6109892,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');

--Provider Welcome Email Redesign 07.02.2023 (ADI-9558)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',1338992,'Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',1329090,'Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',1328668,'Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',6112628,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',6112723,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-02-07','2023-03-13',14,'Provider Welcome Email Redesign',6112881,'202301_Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');

--New Provider Welcome Email 31.03.2023 (ADI-9969)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',1338992,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',1329090,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',1328668,'Email_Trigger_Onboarding_B2C_Seeker_Basic/Premium_AllV_Welcome');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',6508203,'202303_Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',6508268,'202303_Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-03-31','2023-04-21',15,'New Provider Welcome Email',6508284,'202303_Email_Trigger_Onboarding_B2C_Provider_Basic/Premium_AllV_Welcome_M1');

--New Provider Toolkit Email 02.05.2023 (ADI-10152)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',1598780,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',1584066,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',1598383,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',6714258,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',6714335,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-05-02','2023-05-22',16,'New Provider Toolkit Email',6714358,'Email_Trigger_Onboarding_B2C_Provider_Basic_AllV_Toolkit_Test');

--New Seeker Promo Welcome Email 17.05.2023 (ADI-10344)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6109892,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6109596,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6109879,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6840809,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6845817,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-17','2023-05-22',17,'New Seeker Promo Welcome Email',6845891,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');

--Seeker Onboarding Promo Email (Day 2) Redesign 25.05.2023 (ADI-10501)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',5652114,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',5924419,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',5924316,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',6897981,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',6905291,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-15',18,'Seeker Onboarding Promo Email (Day 2) Redesign',6905302,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');

--Promo Code in Job App Trigger 25.05.2023 (ADI-10504)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',2117064,'Transactional-Email_MessageReceived_JobApplication');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',2060591,'Transactional-Email_MessageReceived_JobApplication');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',2060600,'Transactional-Email_MessageReceived_JobApplication');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',6905462,'Transactional-Email_MessageReceived_JobApplicationw/promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',6908865,'Transactional-Email_MessageReceived_JobApplicationw/promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-05-25','2023-06-22',19,'Promo Code in Job App Trigger',6908928,'Transactional-Email_MessageReceived_JobApplicationw/promo');

--30% Promo Code in Abandoned Cart 13.06.2023 for Seeker + Provider (ADI-10626)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1828242,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1828243,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2524045,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1829395,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2524035,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1829402,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2523773,'Transactional-Email_CartAbandoned');

INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1828242,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1828243,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2524045,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1829395,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2524035,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',1829402,'Transactional-Email_CartAbandoned');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',2523773,'Transactional-Email_CartAbandoned');

INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035570,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035647,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035654,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035974,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035985,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7036014,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7036024,'Transactional-Email_CartAbandoned_Test');

INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035570,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035647,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035654,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035974,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7035985,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7036014,'Transactional-Email_CartAbandoned_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-06-13','2023-06-22',20,'30% Promo Code in Abandoned Cart',7036024,'Transactional-Email_CartAbandoned_Test');

--Fast20 Code in Seeker Welcome Email 15.06.2023 (ADI-10653)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',6109596,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',6109892,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',6109879,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',7054308,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',7054455,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-15','2023-06-23',21,'Fast20 Code in Seeker Welcome Email',7054476,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_M1');

--Promo in AutoApp Received 27.06.2023 (ADI-10750)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-27','2023-07-06',22,'Promo in AutoApp Received',3970185,'Transactional-Email_AutoAppReceived_Profile and Transactional-Email_AutoAppReceived_Inbox');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-27','2023-07-06',22,'Promo in AutoApp Received',3970184,'Transactional-Email_AutoAppReceived_Profile and Transactional-Email_AutoAppReceived_Inbox');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-27','2023-07-06',22,'Promo in AutoApp Received',7135269,'Transactional-Email_AutoAppReceived_w/promo_Profile and Transactional-Email_AutoAppReceived_w/promo_Inbox');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-27','2023-07-06',22,'Promo in AutoApp Received',7135297,'Transactional-Email_AutoAppReceived_w/promo_Profile and Transactional-Email_AutoAppReceived_w/promo_Inbox');

--Promo in Message Received 29.06.2023 (ADI-10758)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',2117023,'Transactional-Email_MessageReceived_NewMessage');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',2059904,'Transactional-Email_MessageReceived_NewMessage');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',2059913,'Transactional-Email_MessageReceived_NewMessage');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',7137350,'Transactional-Email_MessageReceived_NewMessage_w/promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',7143938,'Transactional-Email_MessageReceived_NewMessage_w/promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-06-29','2023-07-06',23,'Promo in Message Received',7144001,'Transactional-Email_MessageReceived_NewMessage_w/promo');

--FAST20 + US Copy in Seeker Welcome Email (ADI-10788)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',6109596,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',6109892,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',6109879,'Email_Trigger_Onboarding_B2C_Seeker/Provider_Basic/Premium_AllV_Welcome_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',7191807,'202306_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',7191775,'202306_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-05','2023-07-20',24,'FAST20 + US Copy in Seeker Welcome Email',7186479,'202306_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');

--Providers Near You View CTA (ADI-10795)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-06','2023-07-20',25,'Providers Near You View CTA',7189783,'Transactional-Email_ProviderNearYou_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-07-06','2023-07-20',25,'Providers Near You View CTA',7189781,'Transactional-Email_ProviderNearYou_ViewCTA_Test');

--Jobs Near You SL (ADI-10840)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-07-13','2023-07-17',26,'Jobs Near You SL',2044649,'Transactional-Email_JobsNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-07-13','2023-07-17',26,'Jobs Near You SL',7247803,'Transactional-Email_JobsNearYou_CTA-up_TestSL');

--Jobs Near You CTA: See Details (ADI-10932)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-07-26','2023-07-31',27,'Jobs Near You CTA See Details',2044649,'Transactional-Email_JobsNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-07-26','2023-07-31',27,'Jobs Near You CTA See Details',7342096,'Transactional-Email_JobsNearYou_CTATest');

--Jobs Near You CTA: Be the First to Apply (ADI-11031)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-03','2023-08-08',28,'Jobs Near You CTA: Be the First to Apply',2044649,'Transactional-Email_JobsNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-03','2023-08-08',28,'Jobs Near You CTA: Be the First to Apply',7395184,'Transactional-Email_JobsNearYou_Banner');

--Abandoned Rate Card Day0 Exclusion: Seeker + Provider (ADI-11193)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7562528,'Transactional-Email_AbandonedRateCard');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7562529,'Transactional-Email_AbandonedRateCard_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7562528,'Transactional-Email_AbandonedRateCard');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7562529,'Transactional-Email_AbandonedRateCard_Rem');

INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7279166,'Transactional-Email_AbandonedRateCard');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7279169,'Transactional-Email_AbandonedRateCard_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7279166,'Transactional-Email_AbandonedRateCard');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('provider','2023-08-24','2023-09-06',29,'Abandoned Rate Card Day0 Exclusion',7279169,'Transactional-Email_AbandonedRateCard_Rem');

--Abandoned Rate Card / Onboarding Promo Day 2 (ADI-11478)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-10-04','2023-10-19',30,'Abandoned Rate Card / Onboarding Promo Day 2',7755238,'Transactional-Email_AbandonedRateCard_Day1_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-10-04','2023-10-19',30,'Abandoned Rate Card / Onboarding Promo Day 2',7755235,'Transactional-Email_AbandonedRateCard_Nth_Upgrade_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-10-04','2023-10-19',30,'Abandoned Rate Card / Onboarding Promo Day 2',6905291,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-10-04','2023-10-19',30,'Abandoned Rate Card / Onboarding Promo Day 2',6905302,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-10-04','2023-10-19',30,'Abandoned Rate Card / Onboarding Promo Day 2',6897981,'202305_Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Promo_Test');

--Merge Abandoned Rate Card with Welcome for High Intent Day1s (ADI-11820)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-12-07',31,'Merge Abandoned Rate Card with Welcome for High Intent Day1s',8310716,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-12-07',31,'Merge Abandoned Rate Card with Welcome for High Intent Day1s',8310660,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-12-07',31,'Merge Abandoned Rate Card with Welcome for High Intent Day1s',8310839,'Email_Trigger_Onboarding_B2C_Seeker_Basic_AllV_Welcome_w/promo_V3_M1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-12-07',31,'Merge Abandoned Rate Card with Welcome for High Intent Day1s',8311260,'Transactional-Email_AbandonedRateCard_Day1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-12-07',31,'Merge Abandoned Rate Card with Welcome for High Intent Day1s',8284630,'Transactional-Email_AbandonedRateCard_w/Welcome');

--Abandoned Rate Card: Reminder New Design + PNY feed (ADI-11821)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-11-30',32,'Abandoned Rate Card: Reminder New Design + PNY feed',8310480,'Transactional-Email_AbandonedRateCard_Day1_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-11-30',32,'Abandoned Rate Card: Reminder New Design + PNY feed',8310521,'Transactional-Email_AbandonedRateCard_Nth_Upgrade_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-11-30',32,'Abandoned Rate Card: Reminder New Design + PNY feed',8248263,'Transactional-Email_AbandonedRateCard_Day1_Rem_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-16','2023-11-30',32,'Abandoned Rate Card: Reminder New Design + PNY feed',8248271,'Transactional-Email_AbandonedRateCard_Nth_Upgrade_Rem_Test');

--Provider Profile Viewed - Improve Unsubs (ADI-11887)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-20','2024-03-06',33,'Provider Profile Viewed - Improve Unsubs',8016204,'Transactional-Email_ProviderProfileViewed_A');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-11-20','2024-03-06',33,'Provider Profile Viewed - Improve Unsubs',8336343,'Transactional-Email_ProviderProfileViewed_B');

--Abandoned Rate Card Dedicated Reupgrades Copy (ADI-11925)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-12-11','2024-01-08',34,'Abandoned Rate Card Dedicated Reupgrades Copy',8513776,'Transactional-Email_AbandonedRateCard_Nth_Reupgrade');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2023-12-11','2024-01-08',34,'Abandoned Rate Card Dedicated Reupgrades Copy',8487167,'Transactional-Email_AbandonedRateCard_Nth_Reupgrade_Test');

--Valentines Day Promo DE (ASB-591)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-02-06','2024-02-07',35,'Valentines Day Promo DE',8911651,'20240206_Email_Blast_B2C_Seeker_Basic_AllV_Promo_PreValentinesDay_HoldOut_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-02-06','2024-02-07',35,'Valentines Day Promo DE',8911664,'20240206_Email_Blast_B2C_Seeker_Basic_AllV_Promo_PreValentinesDay_Control_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-02-06','2024-02-07',35,'Valentines Day Promo DE',8911670,'20240206_Email_Blast_B2C_Seeker_Basic_AllV_Promo_PreValentinesDay_Test_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-02-06','2024-02-07',35,'Valentines Day Promo DE',8931912,'20240207_Email_Blast_B2C_Seeker_Basic_AllV_Promo_PreValentinesDay_Control_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-02-06','2024-02-07',35,'Valentines Day Promo DE',8931913,'20240207_Email_Blast_B2C_Seeker_Basic_AllV_Promo_PreValentinesDay_Test_Rem');

--Spring Clean Promo DE/AT/UK/CA (ASB-12499)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-05','2024-03-06',36,'Spring Clean Promo DE/AT/UK/CA',9140854,'20240305_Email_Blast_B2C_Seeker_Basic_HKCC_Promo_SpringCleanYourCastle_25%off_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-05','2024-03-06',36,'Spring Clean Promo DE/AT/UK/CA',9141692,'20240306_Email_Blast_B2C_Seeker_Basic_HKCC_Promo_SpringCleanYourCastle_25%off_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-05','2024-03-06',36,'Spring Clean Promo DE/AT/UK/CA',9141641,'20240305_Email_Blast_B2C_Seeker_Basic_HKCC_Promo_SpringCleanYourCastle_50%off_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-05','2024-03-06',36,'Spring Clean Promo DE/AT/UK/CA',9141693,'20240306_Email_Blast_B2C_Seeker_Basic_HKCC_Promo_SpringCleanYourCastle_50%off_Rem');

--AutoApp Received SL (ADI-12516)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-03-28',37,'AutoApp Received SL',9215748,'Transactional-Email_AutoAppReceived_Profile');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-03-28',37,'AutoApp Received SL',9215815,'Transactional-Email_AutoAppReceived_Inbox');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-03-28',37,'AutoApp Received SL',9215756,'Transactional-Email_AutoAppReceived_Profile_Test');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-03-28',37,'AutoApp Received SL',9215807,'Transactional-Email_AutoAppReceived_Inbox_Test');

--Promo in PNY for Marketing Opt-Ins (ASB-1145)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-10-08',38,'PNY Promo for Marketing Opt-Ins',9258591,'Transactional-Email_ProviderNearYou_mktg-control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-12','2024-10-08',38,'PNY Promo for Marketing Opt-Ins',9258585,'Transactional-Email_ProviderNearYou_mktg-test-w-promo20');

--Promo and PNY in DE (ADI-12524)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-19','2024-10-08',39,'Promo and PNY in DE',9302095,'Transactional-Email_ProviderNearYou_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-19','2024-10-08',39,'Promo and PNY in DE',9302105,'20240319_Email_Blast_B2C_Seeker_Basic_AllV_Promo_WelcomeSpringAutumn_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-19','2024-10-08',39,'Promo and PNY in DE',9302116,'Transactional-Email_ProviderNearYou_w/promocode');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-20','2024-10-08',39,'Promo and PNY in DE',9302281,'Transactional-Email_ProviderNearYou_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-20','2024-10-08',39,'Promo and PNY in DE',9302283,'20240320_Email_Blast_B2C_Seeker_Basic_AllV_Promo_WelcomeSpringAutumn_Rem');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-03-20','2024-10-08',39,'Promo and PNY in DE',9302282,'Transactional-Email_ProviderNearYou_w/promocode');

--Job Posted Confirmation (ADI-12560)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-04-23','2024-10-08',40,'Job Posted Confirmation',9654694,'Email_Trigger_JobPosted_B2C_Seeker_Basic/Premium_AllV_JobPostedConfirmation_HoldOutGroup');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-04-23','2024-10-08',40,'Job Posted Confirmation',9655287,'Email_Trigger_JobPosted_B2C_Seeker_Basic/Premium_AllV_JobPostedConfirmation');

--Robust Provider Profile Snippets in PNY (ASB-2091)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-05-08','2024-06-11',41,'Robust Provider Profile Snippets in PNY',9784800,'Transactional-Email_ProviderNearYou_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-05-08','2024-06-11',41,'Robust Provider Profile Snippets in PNY',9784831,'Transactional-Email_ProviderNearYou_TEST-RobustProfileSnippet');

--My Carers LP in AutoApp Received Email (ASB-2141)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-05-13','2024-10-08',42,'My Carers LP in AutoApp Received Email',9820124,'Transactional-Email_AutoAppReceived_Control_LP-Profile');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-05-13','2024-10-08',42,'My Carers LP in AutoApp Received Email',9820130,'Transactional-Email_AutoAppReceived_Control_LP-Inbox');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-05-13','2024-10-08',42,'My Carers LP in AutoApp Received Email',9820060,'Transactional-Email_AutoAppReceived_Test_LP-MyCarers');

--PNY Robust Snippet DACH Markets (ADI-12608)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-06-18','2024-10-08',43,'PNY Robust Snippet DACH Markets',10181185,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Control-V2');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-06-18','2024-10-08',43,'PNY Robust Snippet DACH Markets',10160604,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Test-V2');

--PNY Robust Snippet EN Markets (ADI-12609)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-06-18','2024-10-08',44,'PNY Robust Snippet EN Markets',7189783,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-06-18','2024-10-08',44,'PNY Robust Snippet EN Markets',9784800,'Transactional-Email_ProviderNearYou_Control');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-06-18','2024-10-08',44,'PNY Robust Snippet EN Markets',10160786,'Transactional-Email_ProviderNearYou_TEST-RobustProfileSnippet');

--PNY Marketing Module Small Markets (ADI-12610)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-07-09','2024-10-08',45,'PNY Marketing Module Small Markets',10390714,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-07-09','2024-10-08',45,'PNY Marketing Module Small Markets',10390708,'Transactional-Email_ProviderNearYou_MktgModule');

--AutoApp Received Robust provider Snippet V1 (INTLADI-58)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-09-25','2024-11-12',46,'AutoApp Received Robust provider Snippet V1',11172827,'Transactional-Email_AutoAppReceived_Test_LP-MyCarers');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-09-25','2024-11-12',46,'AutoApp Received Robust provider Snippet V1',11172816,'Transactional-Email_AutoAppReceived_RobustProfileSnippets_V1');

--PNY Robust provider Snippet V3 (INTLADI-59)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-09','2024-11-12',47,'PNY Robust provider Snippet V3',11318551,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Test-V2');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-09','2024-11-12',47,'PNY Robust provider Snippet V3',11306131,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Test-V3');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-09','2024-11-12',47,'PNY Robust provider Snippet V3',11306084,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Test-V3');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-09','2024-11-12',47,'PNY Robust provider Snippet V3',11306150,'Transactional-Email_ProviderNearYou_RobustProfileSnippet_Test-V3');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-09','2024-11-12',47,'PNY Robust provider Snippet V3',11318536,'Transactional-Email_ProviderNearYou_TEST-RobustProfileSnippet');

--PNY Robust provider Snippet No Photo (INTLADI-60)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-23','2024-11-12',48,'PNY Robust provider Snippet No Photo',11461706,'Transactional-Email_ProviderNearYou');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-10-23','2024-11-12',48,'PNY Robust provider Snippet No Photo',11461281,'Transactional-Email_ProviderNearYou_TEST-RobustProfileSnippet_V1_NoPhoto');

--AutoApp Received Robust Provider Snippet V2 (INTLADI-64)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-11-26','2024-12-11',49,'AutoApp Received Robust Provider Snippet V2',11819909,'Transactional-Email_AutoAppReceived_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-11-26','2024-12-11',49,'AutoApp Received Robust Provider Snippet V2',11819846,'Transactional-Email_AutoAppReceived_RobustProfileSnippets_V2');

--JobApp Robust Provider Snippet V1 (INTLADI-68)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069244,'Transactional-Email_MessageReceived_JobApplication');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12064158,'Transactional-Email_MessageReceived_JobApplication');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069286,'Transactional-Email_MessageReceived_JobApplication_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069229,'Transactional-Email_MessageReceived_JobApplication_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12064161,'Transactional-Email_MessageReceived_JobApplication_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069267,'Transactional-Email_MessageReceived_JobApplication_W/Promo_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069203,'Transactional-Email_MessageReceived_JobApplication_W/Promo_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12067845,'Transactional-Email_MessageReceived_JobApplication_W/Promo_RobustProfileSnippets_V1');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069243,'Transactional-Email_MessageReceived_JobApplicationw/promo');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2024-12-27','2050-01-01',50,'JobApp Robust Provider Snippet V1',12069193,'Transactional-Email_MessageReceived_JobApplicationw/promo');

--DE Promo and UGC Content (INTLADI-69)
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2025-01-14','2025-01-27',51,'DE Promo and UGC Content',12255468,'20250114_Email_Blast_B2C_Seeker_Basic_AllV_Promo_NewYearResolutions+UGC');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2025-01-14','2025-01-27',51,'DE Promo and UGC Content',12255517,'20250114_Email_Blast_B2C_Seeker_Basic_AllV_Promo_NewYearResolutions_Launch');
INSERT INTO analytics_prod.INTL_AB_CRM_Performance VALUES ('seeker','2025-01-14','2025-01-27',51,'DE Promo and UGC Content',12255508,'20250120_Email_Blast_B2C_Seeker_Basic_AllV_Content_UGC');


GRANT ALL on analytics_prod.INTL_AB_CRM_Performance to reporting_ro, analytics_team WITH GRANT OPTION;

select * from analytics_prod.INTL_AB_CRM_Performance;

select country, count(*) from intl.DW_F_MEMBER_EMAIL_EVENT_DETAIL_INTL
where campaign_id = '3728784' --campaign_name = '20220222_Email_Blast_B2C_Seeker_Basic_AllV_Promo_ADealtoHelpYouOut_Launch'
 group by 1;

with event_data as (
select testid, campaign_name, variation, title, target, startdate, enddate, -- country, 
  sum(Sends) as Sends, sum(Opens) as Opens, sum(Clicks) as Clicks, sum(Unsubs) as Unsubs,
  sum(Providers_With_Profile) as 'Providers with Profile',
  sum(Complete_Profiles) as 'Complete Profiles',
  sum(Apps) as 'Manual Apps',
  sum(Review_Requesters) as 'Review Requesters',
  sum(Reviews_Requested) as 'Reviews Requested',
  sum(Reviews_Created) as 'Reviews Created',
  sum(uploadedPhoto) as 'Uploaded Photo',
  sum(memberApprovedPhoto) as 'Has Approved Photo',
  sum(Upgrades) as Upgrades,
  sum(Day1s) as Day1s,
  sum(Nths) as Nths,
  sum(Reupgrades) as Reupgrades,
  --sum(t.amount) as amount,
  sum(t.amount * fx.currency_rate) as 'USD Bookings',
  --sum(case when pc.promoCode is not null then t.amount end) as amountPC,
  --sum(case when pc.promoCode is not null then t.amount * fx.currency_rate end) as amountPCUSD,
  count(distinct case when pc.promoCode is not null then objectid end) as 'Code Redemptions'
from (
  select det.country, det.campaign_name,
    ab.testid, cc.variation, ab.title,  ab.target, ab.startdate, ab.enddate,
    up.objectid, date(up.datecreated) as upgrade_date, up.promocode, --det.campaign_id, det.country, --up.memberid,
    --date(det.event_datetime) as date,
    count(distinct case when det.event_type = 'Sent' then det.member_id end) as Sends,
    count(distinct case when det.event_type = 'Open' then det.member_id end) as Opens,
    count(distinct case when det.event_type = 'Click Through' then det.member_id end) as Clicks,
    count(distinct case when det.event_type in ('Opt Out','Web Alerts Opt Out','Web Emails Opt Out','Web Newsletter Opt Out') then det.member_id end) as Unsubs,
    count(distinct case when pr.name in ('ProfileCreate','ProfileActivate','ProfileAdd') then pr.memberid end) as Providers_With_Profile,
    count(distinct case when pr.name in ('ProfileCreate','ProfileActivate','ProfileAdd') then pr.profileid end) as Complete_Profiles,
    count(distinct case when pr.name = 'JobApplication' and pr.issystemgenerated is null then pr.jobapplicationid end) as Apps,
    count(distinct rev.requesting_member_id) as Review_Requesters,
    count(distinct rev.request_id) as Reviews_Requested,
    count(distinct rev.reviewid) as Reviews_Created,
    count(distinct case when pr.name = 'Photo' and pr.photoaction = 'Upload' then pr.memberid end) as uploadedPhoto,
    count(distinct ph.memberid) as memberApprovedPhoto,
    count(distinct up.memberid) as Upgrades,
    count(distinct case when up.itemType = 'day1Premium' then up.memberid end) as Day1s,
    count(distinct case when up.itemType = 'dayNPremium' then up.memberid end) as Nths,
    count(distinct case when up.itemType = 'Reupgrade' then up.memberid end) as Reupgrades
  from intl.DW_F_MEMBER_EMAIL_EVENT_DETAIL_INTL det
    join analytics_prod.INTL_AB_CRM_Performance ab on det.event_datetime between ab.startdate and ab.enddate
      and case when ab.campaign_id = 0 then ab.campaign_name = det.campaign_name else ab.campaign_id = det.campaign_id end
    join analytics_prod.INTL_AB_CRM_Cell_Config cc on cc.testid = ab.testid and right(cast(det.member_id as varchar), 1) = cc.memberidLastDigit
      and case when cc.campaignName <> '' then cc.campaignName = det.campaign_name else 1=1 end
    left join intl.hive_event pr on det.member_id = pr.memberid and det.country = pr.countrycode and det.campaign_id = concat(pr.campaignname, pr.adgroupname)
      and pr.name in ('ProfileCreate','ProfileActivate','ProfileAdd','JobApplication','Photo')
      and pr.datecreated between ab.startdate and ab.enddate
      and pr.year >= 2021 and pr.datecreated >= '2021-09-30'
    -- Review request
--     left join intl.feedback_request_email fb on fb.country_code = det.country and fb.requesting_member_id = det.member_id 
--       and fb.last_request_sent_date between ab.startdate and ab.enddate 
--       and fb.last_request_sent_date >= '2021-09-30' and fb.status = 'Sent'
    left join (
      select fr.id as request_id, fr.requested_date, fr.country_code, fr.requesting_member_id, fre.id, fre.email_matched_member_id, r.id as reviewid
      from inprogress_intl.feedback_request fr
        left join intl.feedback_request_email fre on fre.country_code = fr.country_code and fre.requesting_member_id = fr.requesting_member_id and fr.id = fre.feedback_request_id
        left join intl.review r on r.country_code = fre.country_code and r.reviewed_member_id = fre.requesting_member_id and r.member_id = fre.email_matched_member_id
          and r.date_created > fr.requested_date and r.search_status = 'Approved'
      where date(fr.requested_date) >= '2021-09-30'
    ) rev on rev.country_code = det.country and rev.requesting_member_id = det.member_id 
        and rev.requested_date between ab.startdate and ab.enddate 
    -- Review created
--     left join intl.hive_event rev on det.member_id = rev.ratedMemberId and det.country = rev.countrycode
--       and rev.name = 'CreateReview'
--       and rev.datecreated between ab.startdate and ab.enddate
--       and rev.year >= 2021 and rev.datecreated >= '2021-09-30'
    -- Photo Apprvoal
    left join intl.hive_event ph on ph.memberid = pr.memberid and ph.countrycode = pr.countrycode and ph.datecreated > pr.datecreated and ph.photoid = pr.photoid
      and ph.name = 'Photo' and ph.photoaction = 'Approve'
      and ph.datecreated between ab.startdate and ab.enddate
      and ph.year >= 2021 and ph.datecreated >= '2021-09-30'
    left join intl.hive_event up on up.countrycode = det.country and up.memberid = det.member_id --and date(det.event_datetime) = date(up.datecreated)
      and up.name = 'Upgrade' --and det.event_type = 'Click Through' 
      and up.datecreated between ab.startdate and ab.enddate
      and up.year >= 2021 and up.datecreated >= '2021-09-30'
      and det.campaign_id = concat(up.campaignname, up.adgroupname) 
  where
    det.event_type in ('Sent','Open','Click Through','Opt Out','Web Alerts Opt Out','Web Emails Opt Out','Web Newsletter Opt Out')
    and date(det.event_datetime) <> current_date -- and det.campaign_id = '3007320'
  group by 1,2,3,4,5,6,7,8,9,10,11
) det 
  left join intl.transaction t on t.country_code = det.country and t.subscription_plan_id = det.objectid and date(t.date_created) = det.upgrade_date
    and t.type in ('PriorAuthCapture', 'AuthAndCapture') and t.status = 'SUCCESS' and t.amount > 0
  left join intl.hive_subscription_plan sp on t.subscription_plan_id = sp.subscriptionId and t.country_code = sp.countrycode   
  left join reporting.dw_care_fx_rates fx on sp.currency = fx.source_currency and fx.target_currency = 'USD'
  left join analytics_prod.INTL_AB_CRM_Promocodes pc on pc.promocode = det.promocode
group by 1,2,3,4,5,6,7
)
