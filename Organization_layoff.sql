create database layoff;
show databases;
use layoff;
-- loading table from excel
show tables;
select * from layoffs;
-- data cleaning 
-- creating a duplicate original data to keep the raw data secure
create table layoff2
like layoffs;
select *from layoff2;
insert layoff2
select * from layoffs;
select * from layoff2;
-- using row_number and partition to identify duplicate
select *, row_number() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, Stage,
funds_raised_millions,`date`, country) as roll_number
from layoff2;
-- checking roll_number that is more than 1
with roll_number2 AS(
select *, row_number() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, Stage,
funds_raised_millions,`date`, country) as roll_number
from layoff2)
select* from
roll_number2 where roll_number >1;
-- There is about 5 rows with row number more than 1. which indicate duplicate, hence we need to delete the duplicate.
-- CTE cannot delete row thus, we create new table insert the new cte table called roll_number2 in order to delete rows with 2
drop table if exists row_num2;
CREATE TABLE `row_num2`(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `roll_number2` int
);

select * from row_num2;
insert  row_num2
select *, row_number() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, Stage,
funds_raised_millions,`date`, country) as roll_number
from layoff2;
select* from row_num2;
 
 -- confriming the duplicate
select * from row_num2
where company = 'casper';

select * from row_num2
where roll_number2 > 1;
-- we have 5 rows with roll_number more than 1, this means that number 2 is the duplicate row and should b deleted

DELETE
 from row_num2
where roll_number2 > 1;

select * from row_num2
where roll_number2>1;
select * from row_num2;
-- successfully deleted all the duplicate
-- stage 2 data standardization
 select * from row_num2;
 -- triming the company's name to remove spaces before and after the names then update it
 select trim(company)
 from row_num2;
update row_num2
set company = trim(company);

 select * from row_num2;
-- Because industry is like a category each company belongs, so we need to distinct it for proper grouping
select distinct industry
from row_num2 order by 1;
select * from row_num2
Where industry like 'crypto%';
update row_num2
set industry = 'crypto'
where industry like 'crypto%';

select distinct industry
from row_num2 order by 1;

select distinct location
 from row_num2;

select trim( trailing '.' from location)
from row_num2 order by 1;
update row_num2
set location = trim( trailing '.' from location);

select * from row_num2;
select distinct country
from row_num2 order by 1;

select distinct trim(trailing '.' from country)
from row_num2 order by 1;
update row_num2
set country = trim(trailing '.' from country);
 -- changing the date formate and updating it
 select `date`, str_to_date(`date`, '%m/%d/%Y')
 from row_num2;
 update row_num2
 set `date` = str_to_date(`date`, '%m/%d/%Y');
-- changing the date formate
alter table row_num2
modify column `date`  date;

-- updating industry with space and null using the category of similar company
select industry, company  from row_num2
order by 1; 
select * from row_num2
where company =  'carvana';

update row_num2
set industry = null
where industry ='';

select * from row_num2
where industry is null;

-- joining the companies with null industry with similar company in a particular industry
-- eg there is airbnb with null industry so i have to join it with another airbnb under travel industry
select t1.company, t1.industry, t2.industry
from row_num2 t1
join row_num2 t2
on t1.company= t2.company
where t1.industry is null and
t2.industry is not null;
update row_num2 t1
join row_num2 t2 
on t1.company= t2.company
set t1.industry = t2.industry
where t1.industry is null and
t2.industry is not null;

select industry from row_num2
order by 1;

-- removing unwanted column

select * from row_num2;
alter table row_num2
drop column roll_number2;


-- removing some unnecessary null values
select * from row_num2
where total_laid_off is null and 
percentage_laid_off is null;

delete  from row_num2
where total_laid_off is null and 
percentage_laid_off is null;

select  company,  max(total_laid_off) from row_num2
group by company
order by 2;

select  industry,  max(total_laid_off) from row_num2
group by industry
order by 2 desc;
SELECT 
    country, MAX(total_laid_off)
FROM
    row_num2
GROUP BY country
ORDER BY 2 DESC;

select   year(`date`),
sum(total_laid_off) from row_num2
group by year(`date`)
order by 2;

	select country, sum(total_laid_off)
	as Total_laied_in_2022
    from row_num2
	where year(`date`)= '2022'
	group by country 
	order by 2 desc;
    
    select sum(total_laid_off) from row_num2
    where year (`date`) = '2022';
    
    with percentage_2022 as(
    select country, sum(total_laid_off)
	as Total_laidoff_2022
    from row_num2
	where year(`date`)= '2022'
	group by country 
	order by 2 desc)
    select country, total_laidoff_2022,  (total_laidoff_2022 *100)/ 
    (select sum(total_laid_off) from row_num2
    where year (`date`) = '2022') as 'percentage_laidoff_2022'
    from percentage_2022
    group by country;
    
-- creating a table to save our analysis
drop table if exists laidoff_2022;
create table laidoff_2022 (Country varchar (255), total_laidoff_2022 int, percentage_laidoff_2022 int);
insert laidoff_2022
with percentage_2022 as(
select country, sum(total_laid_off)
	as Total_laidoff_2022
    from row_num2
	where year(`date`)= '2022'
	group by country 
	order by 2 desc)
    select country, total_laidoff_2022,  (total_laidoff_2022 *100)/ 
    (select sum(total_laid_off) from row_num2
    where year (`date`) = '2022') as 'percentage_laidoff_2022'
    from percentage_2022
    group by country;
    select * from laidoff_2022;