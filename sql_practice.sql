-- ============================================
-- SQL PRACTICE LOG — KARTIK
-- SQLZoo Quizzes: Basics, World, Nobel
-- Day 5 — 90 Day Data Engineering Journey
-- ============================================


-- ============================================
-- SECTION 1: SELECT BASICS (World Table)
-- ============================================
-- Table: world(name, continent, area, population, gdp)

-- Q1: Countries with population between 1000000 and 1250000
SELECT name, population
FROM world
WHERE population BETWEEN 1000000 AND 1250000;

-- Q2: Countries whose name starts with 'Al'
SELECT name, population
FROM world
WHERE name LIKE 'Al%';

-- Q3: Countries ending in 'a' or 'l'
SELECT name FROM world
WHERE name LIKE '%a' OR name LIKE '%l';

-- Q4: European countries with name length = 5
SELECT name, length(name)
FROM world
WHERE length(name) = 5 AND region = 'Europe';

-- Q5: Double the area of country with population 64000
SELECT name, area * 2
FROM world
WHERE population = 64000;

-- Q6: Countries with area > 50000 AND population < 10000000
SELECT name, area, population
FROM world
WHERE area > 50000 AND population < 10000000;

-- Q7: Population density of specific countries
SELECT name, population / area
FROM world
WHERE name IN ('China', 'Nigeria', 'France', 'Australia');


-- ============================================
-- SECTION 2: SELECT FROM WORLD (BBC Quiz)
-- ============================================

-- Q1: Countries beginning with 'U'
SELECT name
FROM world
WHERE name LIKE 'U%';

-- Q2: Population of United Kingdom
SELECT population
FROM world
WHERE name = 'United Kingdom';

-- Q3: Name and population of countries in Europe and Asia
SELECT name, population
FROM world
WHERE continent IN ('Europe', 'Asia');

-- Q4: Show two specific countries using IN
SELECT name
FROM world
WHERE name IN ('Cuba', 'Togo');

-- Q5: South American countries with population > 40 million
SELECT name
FROM world
WHERE continent = 'South America'
AND population > 40000000;

-- Q6: Population divided by 10 for small countries
SELECT name, population / 10
FROM world
WHERE population < 10000;


-- ============================================
-- SECTION 3: SELECT FROM NOBEL
-- ============================================
-- Table: nobel(yr, subject, winner)

-- Q1: Winners beginning with 'C' and ending in 'n'
SELECT winner
FROM nobel
WHERE winner LIKE 'C%' AND winner LIKE '%n';

-- Q2: Count Chemistry awards between 1950 and 1960
SELECT COUNT(subject)
FROM nobel
WHERE subject = 'Chemistry'
AND yr BETWEEN 1950 AND 1960;

-- Q3: Years where no Medicine awards were given
SELECT COUNT(DISTINCT yr)
FROM nobel
WHERE yr NOT IN (
    SELECT DISTINCT yr
    FROM nobel
    WHERE subject = 'Medicine'
);

-- Q4: Winners with 'Sir' in name from 1960s
SELECT subject, winner
FROM nobel
WHERE winner LIKE 'Sir%'
AND yr LIKE '196%';

-- Q5: Years when neither Physics nor Chemistry award was given
SELECT yr
FROM nobel
WHERE yr NOT IN (
    SELECT yr
    FROM nobel
    WHERE subject IN ('Chemistry', 'Physics')
);

-- Q6: Years when Medicine given but NOT Peace or Literature
SELECT DISTINCT yr
FROM nobel
WHERE subject = 'Medicine'
AND yr NOT IN (
    SELECT yr FROM nobel WHERE subject = 'Literature'
)
AND yr NOT IN (
    SELECT yr FROM nobel WHERE subject = 'Peace'
);

-- Q7: Count of awards per subject in 1960
SELECT subject, COUNT(subject)
FROM nobel
WHERE yr = '1960'
GROUP BY subject;


-- ============================================
-- KEY CONCEPTS LEARNED TODAY
-- ============================================

-- LIKE operator:
--   'U%'  = starts with U
--   '%a'  = ends with a
--   '%a%' = contains a

-- BETWEEN: inclusive range filter
--   WHERE yr BETWEEN 1950 AND 1960

-- IN: match against a list
--   WHERE name IN ('Cuba', 'Togo')

-- LENGTH(): filter by string length
--   WHERE length(name) = 5

-- COUNT() + GROUP BY: aggregation
--   SELECT subject, COUNT(subject) FROM nobel GROUP BY subject

-- Subqueries with NOT IN:
--   WHERE yr NOT IN (SELECT yr FROM nobel WHERE ...)

-- Arithmetic in SELECT:
--   SELECT name, population/area FROM world
