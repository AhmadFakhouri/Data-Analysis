
-- 			Remove duplicates 

create table layoffs2
Like layoffs;

select * 
from layoffs2;

Insert layoffs2
select * 
from layoffs;


with cte as (
select * , 
row_number() over(partition by company  , location , industry , total_laid_off , percentage_laid_off , `date` , stage , country , funds_raised_millions) as row_num
from layoffs2
)


delete 
from  layoffs2
where row_num >1;

CREATE TABLE `layoffs_final` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_final
select * , 
row_number() over(partition by company  , location , industry , total_laid_off , percentage_laid_off , `date` , stage , country , funds_raised_millions) as row_num
from layoffs2;

delete 
from layoffs_final
where row_num >1;

select * -- 				Test Delete statment 
from layoffs_final
where row_num >1;
-- -----------------------------------------------

--          				Standardizing Data

select distinct(trim(company))
from layoffs_final;

update layoffs_final
set company = trim(company);


select distinct(industry)
from layoffs_final
order by 1;

update layoffs_final
 set industry = 'Crypto'
 where industry like 'Crypto%';


select  distinct country , trim(country)
from layoffs_final
where country like '%united%'
order by 1;

update layoffs_final
set country = 'United States'
where country like 'United States%';


select  distinct country , trim(country)
from layoffs_final
where country like '%united%'
order by 1;

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_final;

update  layoffs_final
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_final 

modify column `date` DATE	 ;
-- ---------------------------------------------

-- 					Null values OR Blank Values

select * 
from layoffs_final 
where company ='Airbnb';


update layoffs_final
set industry = null 
where industry = "";


select t1.industry ,  t2.industry
from layoffs_final t1 
join layoffs_final t2 
on t1.company = t2.company
where (t1.industry is null or t1.industry ="")

 and t2.industry is NOT null ;					

update layoffs_final t1 
join layoffs_final t2
on t1.company = t2.company

SET t1.industry =  t2.industry
where t1.industry is null 
and t2.industry is not null ;
 --  ---------------------------------------------
 
 -- 					Delete unuseful columns 
 
 delete 
 from layoffs_final
 
 where total_laid_off is null and percentage_laid_off is null ;
 
  
  alter table layoffs_final
  drop column row_num; 
        
select * 
from layoffs_final;



-- ------------	 Exploratory Data Analysis (EDA)

select min(`date`) , Max(`date`)
from layoffs_final;

select * 
from layoffs_final
where percentage_laid_off = 1
order by total_laid_off desc;

select company,sum(total_laid_off)
from layoffs_final
group by company 
order by 2 desc ;

select industry ,sum(total_laid_off)
from layoffs_final
group by industry 
order by 2 desc ;

select country,sum(total_laid_off)
from layoffs_final
group by country 
order by 2 desc ;

select year(`date`),sum(total_laid_off)
from layoffs_final
group by year(`date`)
order by 1 desc ;

with rolling_CTE as (
select substring(`date`,1,7) as `Month` , sum(total_laid_off) as total_off
from layoffs_final
where substring(`date`,1,7)  is NOT Null
group by `Month`
order by 1 asc
)
select  `Month` ,total_off,sum(total_off) over (order by `Month`) as Rolling_total
from rolling_CTE;

with cte_test  (company , years , total_laid_off) as 
(
select company,year(`date`),sum(total_laid_off) as total_laid_off
from layoffs_final
group by company , year(`date`)

)

select * , dense_rank() over (partition by years order by total_laid_off desc )as Ranking
from cte_test
where years is not null and total_laid_off is not null
order by Ranking asc;