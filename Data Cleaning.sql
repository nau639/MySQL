-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


SELECT *
FROM world_layoffs.layoffs;

-- Firstly, we need to create staging table for working and cleaning data. The raw data is kept for backup if something happens
CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging
SELECT * FROM world_layoffs.layoffs;

-- Now, we're going to start data cleaning by following these steps:
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Blank and Null Values
-- 4. Remove Unnecessary Columns and Rows


-- 1. Remove Duplicates
# Check for duplicates

SELECT *
FROM world_layoffs.layoffs_staging;

SELECT company, industry, total_laid_off, `date`,
	ROW_NUMBER() OVER (
		PARTITION BY company, industry, total_laid_off, `date`) AS row_num
	FROM world_layoffs.layoffs_staging;
    
SELECT *
FROM (
	SELECT company, industry, total_laid_off, `date`,
	ROW_NUMBER() OVER (
		PARTITION BY company, industry, total_laid_off, `date`) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Sample check for the duplicates
SELECT *
FROM world_layoffs.layoffs_staging
WHERE company = 'Oda';

-- It looks like there are some unique data included that shouldn't be deleted. We need to check every single row to be accurate

-- These are the correct duplicates
SELECT * 
FROM (
	SELECT *,
	ROW_NUMBER() OVER (
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
	FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;


-- We want to remove duplicates where the now_num is >1
-- There are some options to do it, but we will create a new column to add those row_num in. Then delete rows where the row_num is >1

ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT *
FROM world_layoffs.layoffs_staging
;

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;

        
-- Now we can delete the duplicates, the ones with row_num >1

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM world_layoffs.layoffs_staging2 WHERE row_num > 1;



-- 2. Standardize Data

SELECT *
FROM world_layoffs.layoffs_staging2 ;

-- Let's look at company to find if there's something wrong
SELECT company
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- There is some space before the text. We can fix it and make it cleaner
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

-- Let's look at industry to find if there's something wrong
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- There are some similar industry that looks like mispelled as in Crypto. We will try to make it into Crypto
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- We should check all the column to make sure if there's any small mistakes like that. For example, there is another mispelling in country
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- The date column is in text format, we can modify it into date format using these steps
SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

-- Now we can modify the format from text into date
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date`DATE;



-- 2. Blank and Null Values


SELECT *
FROM world_layoffs.layoffs_staging2;

-- We will check for null and blank values first. It seems that industry has some of it that needs to be fixed
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Now we're going to check every single of it if there are others row filled in the same company
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Carvana';

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company = 'Juul';

-- It seems like they have other row's industry filled expect for Bally's
-- Now we will write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- Makes it easy so if there were thousands we wouldn't have to manually check them all

-- Firstly, we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Now check if it has been changed
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- We will populate those nulls
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Now check again for the nulls
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- It seems like only Bally's has no populated row to populate this null values

-- The null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- Having null is easier for calculations during the EDA phase that blank values

-- So there isn't anything I want to change with the null values




-- 4. Remove Unnecessary Columns and Rows


-- We will remove the null total laid off and percentage laid off. I can not be sure if the data can be used, so I will take it out while keeping the raw data safe

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- We do not need row_num anymore, so let's drop it from the table

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2;

