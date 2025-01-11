#  **ETL proces datasetu MovieLens**

Tento repozitár obsahuje implementáciu **ETL** procesu v **Snowflake** pre analýzu dát z **MovieLens** datasetu. Projekt sa zameriava na preskúmanie správania používateľov a ich preferencií vo filmovej oblasti na základe hodnotení filmov a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.


# **1. Úvod a popis zdrojových dát**

Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa filmov, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v preferenciách divákov, najpopulárnejšie filmy a správanie používateľov.

Zdrojové dáta pochádzajú z MovieLens datasetu dostupného tu: [https://grouplens.org/datasets/movielens/](https://grouplens.org/datasets/movielens/). Dataset obsahuje päť tabuliek:

-   movies
    
-   ratings
    
-   users
    
-   occupations
    
-   age_groups
- tags
- genres_movies
- genres
    

Účelom ETL procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

## 1.1 Dátová architektúra
**ERD Diagram**

Stiahnuté dáta máme usporiadané v relačnom modeli, ktorý je znázornený na **entitno-relačnom diagrame (ERD)**

![Obrázok 1 Entitno-relačná schéma MovieLens](https://github.com/TomasZsembera/MovielensDBS/blob/main/MovieLens_ERD.png?raw=true)
*Obrázok 1:  Entitno-relačná schéma MovieLens*
# 2. Dimenzionálny model

Navrhnutý bol hviezdicový model (star schema), pre efektívnu analýzu kde centrálny bod predstavuje faktová tabuľka **fact_ratings**, ktorá je prepojená s nasledujúcimi dimenziami:

**dim_movies**: Obsahuje podrobné informácie o filmoch (názov, rok vydania, žáner). 
**dim_users**: Obsahuje demografické údaje o používateľoch, ako sú vekové kategórie, pohlavie, povolanie a PSČ. 
**dim_date**: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok, názov dňa v týždni). 
**dim_time**: Obsahuje podrobné časové údaje (hodina, minúta, sekunda).
**dim_tags**: Poskytuje tagy ktoré používatelia priradili k daným filmom.

Štruktúra hviezdicového modelu je znázornená na diagrame nižšie. Diagram ukazuje prepojenia medzi faktovou tabuľkou a dimenziami, čo zjednodušuje pochopenie a implementáciu modelu.

![Obrazok 2 Schéma hviezdy pre MovieLens](https://github.com/TomasZsembera/MovielensDBS/blob/main/StarFinal.png?raw=true)
*Obráźok 2: Schéma hviezdy pre MovieLens*
# 3. ETL proces v snowflake

ETL proces bol rozdelený do troch hlavných fáz: extrakcia (Extract), transformácia (Transform) a nahrávanie (Load). Tento proces bol realizovaný v Snowflake a jeho cieľom bolo pripraviť zdrojové dáta v staging vrstve a transformovať ich do viacdimenzionálneho modelu, ktorý je optimalizovaný pre analýzu a vizualizáciu.

## 3.1 Extract (Extrahovanie dát)

Zdrojové dáta vo formáte .csv boli najskôr nahrané do Snowflake prostredníctvom interného stage úložiska s názvom `movielens_stage`. Stage v Snowflake slúži ako dočasné úložisko na importovanie alebo exportovanie dát. Vytvorenie tohto stage bolo zabezpečené použitím nasledujúceho príkazu: 

    CREATE OR REPLACE STAGE movielens_stage;
Do stage boli následne nahraté súbory obsahujúce údaje o filmoch, používateľoch, hodnoteniach, zamestnaniach, veku a tagoch. Dáta boli importované do staging tabuliek pomocou príkazu `COPY INTO`. Pre každú tabuľku sa použil podobný príkaz: 

    COPY INTO users_staging
    FROM @movielens_stage/users.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
   Ak sme chceli pokračovať v procese bez prerušenia pri chybách použili sme: `ON_ERROR = 'CONTINUE'`

##  3.2 Transform (Transformácia dát)

V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

Dimenzie boli navrhnuté na poskytovanie kontextu pre faktovú tabuľku.  `Dim_users`  obsahuje údaje o používateľoch vrátane vekových kategórií, pohlavia a zamestnania. 

    CREATE OR REPLACE TABLE dim_users AS
    SELECT DISTINCT
    u.id AS dim_userid,
    ag.name AS age_group,
    u.gender,
    o.name AS occupation,
    u.zip_code
    FROM users_staging u
    JOIN age_group_staging ag ON u.age = ag.id
    JOIN occupations_staging o ON u.occupation_id = o.id;
    
Dimenzia `dim_date` je navrhnutá tak, aby uchovávala informácie o dátumoch hodnotení filmov. Obsahuje odvodené údaje, ako sú deň, mesiac, rok a deň v týždni (v textovom aj číselnom formáte).

Táto dimenzia je klasifikovaná ako SCD Typ 0, čo znamená, že existujúce záznamy ostávajú nemenné a uchovávajú statické informácie. Ak by sa v budúcnosti objavila potreba sledovať zmeny, napríklad pracovné dni vs. sviatky, je možné prehodnotiť klasifikáciu na SCD Typ 1 (aktualizácia hodnôt) alebo SCD Typ 2 (uchovávanie histórie zmien). Pre aktuálne požiadavky však stačí navrhnutá štruktúra ako SCD Typ 0 s rozširovaním o nové záznamy podľa potreby.

   ```sql
    CREATE OR REPLACE TABLE dim_date AS
    SELECT
    ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateId,
    CAST(rated_at AS DATE) AS date,
    DATE_PART(day, rated_at) AS day,
    CASE DATE_PART(dow, rated_at)
      WHEN 0 THEN 'Nedeľa'
      WHEN 1 THEN 'Pondelok'
	  WHEN 2 THEN 'Utorok'
      WHEN 3 THEN 'Streda'
      WHEN 4 THEN 'Štvrtok'
      WHEN 5 THEN 'Piatok'
      WHEN 6 THEN 'Sobota'
    END AS day_as_string,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year
    FROM ratings_staging;
   ```
 Faktová tabuľka `fact_ratings` obsahuje záznamy o hodnoteniach a prepojenia na všetky dimenzie. Obsahuje kľúčové metriky, ako je hodnota hodnotenia a časový údaj.
 

```sql
CREATE OR REPLACE TABLE fact_rating AS
SELECT
    r.id AS fact_ratingid,
    r.rated_at AS rated_at,
    r.rating,
    COALESCE(LISTAGG(tg.movie_id || '-' || tg.user_id, ',') WITHIN GROUP (ORDER BY tg.movie_id, tg.user_id), '') AS tags,
    d.dim_dateid AS dim_dateid,
    t.dim_timeid AS dim_timeid,
    u.dim_userid AS dim_userid,
    m.dim_movieid AS dim_movieid
FROM ratings_staging r
JOIN dim_date d ON CAST(r.rated_at AS DATE) = d.date
JOIN dim_time t ON DATE_PART(hour, r.rated_at) = t.hour
                AND DATE_PART(minute, r.rated_at) = t.minute
                AND DATE_PART(second, r.rated_at) = t.second
LEFT JOIN dim_users u ON r.user_id = u.dim_userid
LEFT JOIN dim_movies m ON r.movie_id = m.dim_movieid
LEFT JOIN dim_tags tg ON r.movie_id = tg.movie_id
GROUP BY r.id, r.rated_at, r.rating, d.dim_dateid, t.dim_timeid, u.dim_userid, m.dim_movieid
ORDER BY r.id; 
```

## 3.3 Load (Načítanie dát)


Po úspešnom vytvorení dimenzií a faktovej tabuľky boli dáta presunuté do finálnej štruktúry. Staging tabuľky boli následne odstránené, čím sa optimalizovalo využitie úložného priestoru a zabezpečila sa efektivita databázy: 

   ```sql
   DROP TABLE IF EXISTS age_group_staging; 
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;
```
    
 ETL proces v Snowflake umožnil transformáciu pôvodných dát z formátu .csv do viacdimenzionálneho modelu typu hviezda. Tento proces zahŕňal čistenie, obohacovanie a reorganizáciu údajov. Výsledný model poskytuje základ pre analýzu preferencií divákov a ich správania, pričom slúži ako východisko pre vizualizácie a reporty. 

# 4 Vizualizácia dát

Dashboard obsahuje `6 vizualizácií`, ktoré poskytujú základný prehľad o kľúčových metrikách a trendoch týkajúcich sa hodnotení, používateľov a filmov. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť správanie používateľov a ich preferencie.
![Dashboard MovieLens datasetu](https://github.com/TomasZsembera/MovielensDBS/blob/main/DashboardV2.png?raw=true)
*Obrázok 3: Dashboard MovieLens datasetu*
## Graf 1: Top 10 najviac hodnotených filmov

Táto vizualizácia zobrazuje **Top 10 najviac hodnotených filmov**. Výsledok obsahuje zoznam desiatich filmov s najvyšším počtom hodnotení.

-   **Stĺpec `movie_title`** uvádza názvy filmov.
-   **Stĺpec `total_ratings`** ukazuje celkový počet hodnotení, ktoré jednotlivé filmy získali.

Vizualizácia pomáha identifikovať najpopulárnejšie filmy na základe počtu hodnotení od používateľov, čo môže byť užitočné pri analýze trendov sledovanosti alebo plánovaní odporúčaní pre divákov. Filmy sú zoradené podľa počtu hodnotení v zostupnom poradí, aby boli najpopulárnejšie tituly na vrchole zoznamu.

```sql
SELECT 
    m.title AS movie_title,
    COUNT(f.fact_ratingId) AS total_ratings
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
GROUP BY m.title
ORDER BY total_ratings DESC
LIMIT 10;

```


## Graf 2: Najčastejšie používané tagy (TOP 10)
Tento dopyt vráti zoznam 10 najčastejšie používaných tagov v tabuľke `dim_tags`, zoradený podľa počtu ich výskytov (od najvyššieho po najnižší). Pre každý tag sa zobrazuje jeho názov a počet, koľkokrát sa daný tag vyskytuje v tabuľke.

```sql
SELECT 
    t.tag AS tag, 
    COUNT(t.dim_tagid) AS num_uses
FROM dim_tags t
GROUP BY t.tag
ORDER BY num_uses DESC
LIMIT 10;
```



## Graf 3: Trend priemerného hodnotenia filmov podľa roku vydania
Tento dopyt vráti zoznam rokov (vydania filmov) a ich priemerné hodnotenie. Výsledky budú zoradené podľa roku vydania (od najstaršieho po najnovší) a pre každý rok bude zobrazené priemerné hodnotenie všetkých filmov, ktoré boli v danom roku vydané a majú zadané hodnotenie.
```sql
SELECT 
    m.release_year AS year,
    AVG(f.rating) AS avg_rating
FROM fact_ratings f
JOIN dim_movies m ON f.dim_moviesId = m.dim_moviesId
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;

```


## Graf 4: TOP 10 filmov podľa priemerného hodnotenia
Tento dopyt vráti zoznam 10 filmov, ktoré majú aspoň 50 hodnotení. Pre každý film sa zobrazí:

-   jeho názov,
-   priemerné hodnotenie (zaokrúhlené na dve desatinné miesta),
-   počet hodnotení.

Výsledky budú zoradené podľa priemerného hodnotenia, pričom filmy s najvyšším hodnotením budú na začiatku zoznamu.
```sql
SELECT 
    m.title AS movie_title, 
    ROUND(AVG(r.rating), 2) AS average_rating, 
    COUNT(r.rating) AS num_ratings
FROM fact_rating r
JOIN dim_movies m ON r.dim_movieid = m.dim_movieid
GROUP BY m.title
HAVING COUNT(r.rating) >= 50
ORDER BY average_rating DESC
LIMIT 10;

```
## Graf 5: Počet filmov podľa roku vydania
Tento dopyt vráti zoznam rokov vydania filmov a počet filmov, ktoré boli vydané v každom roku. Výsledky budú zoradené od najstaršieho po najnovší rok vydania. Pre každý rok bude zobrazený:

-   rok vydania filmu,
-   počet filmov, ktoré boli v danom roku vydané.
```sql
SELECT release_year, COUNT(*) AS movie_count
FROM dim_movies
GROUP BY release_year
ORDER BY release_year;
```

## Graf 6: Priemerné hodnotenie filmov podľa žánru 
Tento dopyt vráti zoznam žánrov filmov a ich priemerné hodnotenie. Výsledky budú zoradené od žánru s najvyšším priemerným hodnotením po ten s najnižším. Pre každý žáner bude zobrazený:

-   názov žánru,
-   priemerné hodnotenie filmov v tomto žánri.

```sql
SELECT genre, AVG(rating) AS average_rating
FROM fact_ratings fr
JOIN dim_movies dm ON fr.dim_moviesId = dm.dim_moviesId
GROUP BY genre
ORDER BY average_rating DESC;
```

**Autor**: Tomáš Zsembera
