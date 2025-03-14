select * from layoffs;
select count(*) from layoffs;

-- cretaing a copy of the orignal dataset

create table dataset like layoffs;

select  * from dataset ;

insert dataset
select * from layoffs;

select * from dataset;
  
-- removing duplicates
CREATE TABLE `newdata` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE newcte AS 
SELECT *,
       ROW_NUMBER() OVER (
           PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
           ORDER BY `date`
       ) AS row_num 
FROM dataset;

INSERT INTO newdata 
SELECT * FROM newcte;


select count(*) from newdata where total_laid_off is NULL or total_laid_off="";  -- 202

select count(*) from newdata where percentage_laid_off is NULL or percentage_laid_off="";  -- 186

select count(*) from newdata where funds_raised_millions is NULL or funds_raised_millions=""; -- 72

select count(*) from newdata -- 564


select count(*) from newdata 

-- disabling safe sql updates -- 
SELECT @@SQL_SAFE_UPDATES;


SET SQL_SAFE_UPDATES = 0;

-- Standardizing data
-- triming columns to remove extra space 



select * FROM newdata where row_num>1

update newdata
set company=trim(company),
	location=trim(location),
	industry=trim(industry),
	country=trim(country),
    funds_raised_millions=trim(funds_raised_millions),
    stage =trim(stage);


-- standarizing date format
select `date`,str_to_date(`date`,'%m/%d/%Y') from newdata


update newdata
set `date`=str_to_date(`date`,'%m/%d/%Y')

select * from newdata

select count(*) from newdata where (total_laid_off is NULL or total_laid_off="") and (percentage_laid_off is NULL or percentage_laid_off="");  -- 84

select * from newdata where (total_laid_off is NULL or total_laid_off="") and (percentage_laid_off is NULL or percentage_laid_off="");  

delete from newdata 
where (total_laid_off is NULL or total_laid_off="") and (percentage_laid_off is NULL or percentage_laid_off="")


select * from newdata where (total_laid_off is NULL or total_laid_off="") and (percentage_laid_off is NULL or percentage_laid_off="");  

delete from newdata
where industry is null or industry =""

select * from newdata where industry is null or industry =""

select distinct(industry) from newdata order by industry

select distinct(country) from newdata order by country

update newdata
set country="United States" where country like 'United States%'



-- EDA

SELECT * 
FROM newdata
WHERE total_laid_off = (SELECT MAX(total_laid_off) FROM newdata);
-- google - 12000

SELECT * 
FROM newdata
WHERE percentage_laid_off = (SELECT MAX(percentage_laid_off) FROM newdata);
-- 17 companies shutdown percentage_laid_off = 1

SELECT * 
FROM newdata
WHERE funds_raised_millions = (SELECT MAX(funds_raised_millions) FROM newdata);
-- WeWork - 22200

select country,count(*) from newdata group by country order by count(country) desc


SELECT max(`date`)
FROM newdata -- 2023-03-06

SELECT min(`date`)
FROM     -- 2022-12-06

with monthscte as(
SELECT *,left(`date`, 7) AS months
FROM newdata )

select months ,count(*) month_count from monthscte group by months order by month_count desc;
-- 2023-1 => 226

select country ,sum(funds_raised_millions) as total_funds from newdata group by country order by total_funds desc   -- United States =>177475

select industry ,sum(funds_raised_millions) as total_funds from newdata group by industry order by total_funds desc   -- consumer => 65506

select distinct(stage) from newdata

select industry ,sum(total_laid_off) as total_laid from newdata group by industry order by total_laid desc   -- others => 28767


select country ,sum(total_laid_off) as total_laid from newdata group by country order by total_laid desc   -- us => 94955
