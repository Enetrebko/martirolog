from sqlalchemy import create_engine, URL, text
import pandas as pd
import os

env = ".env_prod"

with open(env) as f:
    for l in f.readlines():
        if '=' in l:
            os.environ[l.split('=')[0].strip()] = l.split('=')[1].strip()

db_name = os.environ.get("DATABASE_NAME")
host = os.environ.get("DATABASE_HOST")
port = int(os.environ.get("DATABASE_PORT"))
user = os.environ.get("DATABASE_USER")
password = os.environ.get("DATABASE_PASSWORD")
table_name = os.environ.get("TABLE_NAME")

url_object = URL.create(
    "mysql+pymysql",
    username=user,
    password=password,
    host=host,
    database=db_name,
)

db_connection = create_engine(url_object)


with db_connection.connect() as connection:
    result = connection.execute(text(
        f"""
        select c.title, c.fulltext from {db_name}.kot44_content c
        where introtext = ""
        and alias != "po-mestam-pamyati-tomskoj-oblasti-2022"
        and alias != "grazhdane-litvy";
        """
    )
    )

for res in result.mappings():

    df = pd.read_html(res['fulltext'], extract_links="body")[0]
    df['title'] = res['title']

    for col in df.columns.difference(['Фото плиты']):
        df[col] = df[col].apply(lambda x: x[0])
    df["Фото плиты"] = df["Фото плиты"].apply(lambda x: x[1])

    if "ФИО" not in df.columns:
        df["И"] = df["И"].apply(lambda x: x + "." if not x.endswith(".") else x)
        df["О"] = df["О"].apply(lambda x: "" if x == "-" else x)
        df["О"] = df["О"].apply(lambda x: x + "." if not x.endswith(".") and x != "" else x)
        df["ФИО"] = df["Фамилия"] + " " + df["И"] + df["О"]
    df = df[['ФИО', 'Год рождения', 'Год расстрела', '№ плиты', '№ стелы', 'Фото плиты', 'title']]
    df.columns = ['FIO', 'BIRTH_YEAR', 'EXECUTION_YEAR', 'SLAB_NUMBER', 'STELE_NUMBER', 'SLAB_PHOTO', 'TITLE']

    with db_connection.connect() as connection:
        df.to_sql(name=table_name, con=db_connection, if_exists='append')

with db_connection.connect() as connection:
    _ = connection.execute(text(
        f"""
        ALTER TABLE `{db_name}`.`{table_name}`
        ADD COLUMN `SECTOR_NUMBER` TEXT NULL DEFAULT NULL AFTER `TITLE`,
        ADD COLUMN `SECTOR_PHOTO` TEXT NULL DEFAULT NULL AFTER `SECTOR_NUMBER`,
        ADD COLUMN `STELE_COORD` TEXT NULL DEFAULT NULL AFTER `SECTOR_PHOTO`,
        ADD COLUMN `STELE_COORD_FLOAT` TEXT NULL DEFAULT NULL AFTER `STELE_COORD`,
        ADD COLUMN `STELE_GMAP_LINK` TEXT NULL DEFAULT NULL AFTER `STELE_COORD_FLOAT`;
        """
    )
    )

    _ = connection.execute(text(
        f"""
        UPDATE `{db_name}`.`{table_name}`
        SET SECTOR_NUMBER = case
        when STELE_NUMBER in (1, 2,3,4,5,6,7,8,9,10) then "1"
        when STELE_NUMBER in (11,12,13,14,15,16,17,18,19) then "2"
        when STELE_NUMBER in (20,21,22,23,24,25,26,27) then "3"
        when STELE_NUMBER in (28,29,30,31,32) then "4"
        when STELE_NUMBER in (33,34,35,369) then "5"
        when STELE_NUMBER in (37,38,39,40) then "6"
        when STELE_NUMBER in (41,42,43,44,45,46) then "7"
        else "" end,
        
        SECTOR_PHOTO = case 
        when STELE_NUMBER in (1, 2,3,4,5,6,7,8,9,10) then "karty/sector1.png"  
        when STELE_NUMBER in (11,12,13,14,15,16,17,18,19) then "karty/sector2.png" 
        when STELE_NUMBER in (20,21,22,23,24,25,26,27) then "karty/sector3.png" 
        when STELE_NUMBER in (28,29,30,31,32) then "karty/sector4.png" 
        when STELE_NUMBER in (33,34,35,369) then "karty/sector5.png" 
        when STELE_NUMBER in (37,38,39,40) then "karty/sector6.png" 
        when STELE_NUMBER in (41,42,43,44,45,46) then "karty/sector7.png" 
        else "" end,
        
        SLAB_PHOTO = replace(SLAB_PHOTO, 'images/martirolog/',''),
        
        STELE_COORD_FLOAT = case 
        when STELE_NUMBER = 1 then '56.828139, 60.430694'
        when STELE_NUMBER = 2 then '56.828083, 60.430667'
        when STELE_NUMBER = 3 then '56.828028, 60.430694'
        when STELE_NUMBER = 4 then '56.828028, 60.430806'
        when STELE_NUMBER = 5 then '56.828000, 60.431000'
        when STELE_NUMBER = 6 then '56.828056, 60.431111'
        when STELE_NUMBER = 7 then '56.828139, 60.431139'
        when STELE_NUMBER = 8 then '56.828222, 60.431111'
        when STELE_NUMBER = 9 then '56.828278, 60.430972'
        when STELE_NUMBER = 10 then '56.828333, 60.430889'
        when STELE_NUMBER = 11 then '56.828278, 60.431222'
        when STELE_NUMBER = 12 then '56.828306, 60.431333'
        when STELE_NUMBER = 13 then '56.828389, 60.431500'
        when STELE_NUMBER = 14 then '56.828472, 60.431583'
        when STELE_NUMBER = 15 then '56.828500, 60.431667'
        when STELE_NUMBER = 16 then '56.828583, 60.431583'
        when STELE_NUMBER = 17 then '56.828556, 60.431500'
        when STELE_NUMBER = 18 then '56.828556, 60.431417'
        when STELE_NUMBER = 19 then '56.828528, 60.431250'
        when STELE_NUMBER = 20 then '56.828556, 60.431000'
        when STELE_NUMBER = 21 then '56.828583, 60.431167'
        when STELE_NUMBER = 22 then '56.828667, 60.431389'
        when STELE_NUMBER = 23 then '56.828639, 60.431194'
        when STELE_NUMBER = 24 then '56.828694, 60.431250'
        when STELE_NUMBER = 25 then '56.828722, 60.431028'
        when STELE_NUMBER = 26 then '56.828750, 60.431139'
        when STELE_NUMBER = 27 then '56.828833, 60.430861'
        when STELE_NUMBER = 28 then '56.828861, 60.430722'
        when STELE_NUMBER = 29 then '56.828750, 60.430778'
        when STELE_NUMBER = 30 then '56.828806, 60.430694'
        when STELE_NUMBER = 31 then '56.828861, 60.430472'
        when STELE_NUMBER = 32 then '56.828806, 60.430500'
        when STELE_NUMBER = 33 then '56.828806, 60.430194'
        when STELE_NUMBER = 34 then '56.828806, 60.430056'
        when STELE_NUMBER = 35 then '56.828722, 60.429889'
        when STELE_NUMBER = 36 then '56.828667, 60.429778'
        when STELE_NUMBER = 37 then '56.828528, 60.429778'
        when STELE_NUMBER = 38 then '56.828417, 60.429917'
        when STELE_NUMBER = 39 then '56.828306, 60.430111'
        when STELE_NUMBER = 40 then '56.828306, 60.430250'
        when STELE_NUMBER = 41 then '56.828194, 60.430306'
        when STELE_NUMBER = 42 then '56.828111, 60.430361'
        when STELE_NUMBER = 43 then '56.828056, 60.430417'
        when STELE_NUMBER = 44 then '56.828056, 60.430528'
        when STELE_NUMBER = 45 then '56.828083, 60.430556'
        when STELE_NUMBER = 46 then '56.828167, 60.430583'
        else "" end,
        
        STELE_GMAP_LINK = case 
        when STELE_NUMBER = 1 then 'https://maps.app.goo.gl/27ng7k4AMSo2oyPG8'
        when STELE_NUMBER = 2 then 'https://maps.app.goo.gl/ich7ytFKnhm2zSK48'
        when STELE_NUMBER = 3 then 'https://maps.app.goo.gl/jHC6kJ7ZMY2ntBcP9'
        when STELE_NUMBER = 4 then 'https://maps.app.goo.gl/kCoXn4vfwxnio63T6'
        when STELE_NUMBER = 5 then 'https://maps.app.goo.gl/vDgk8PCYZAGGAtb57'
        when STELE_NUMBER = 6 then 'https://maps.app.goo.gl/oEzEXXkGqxR1GC748'
        when STELE_NUMBER = 7 then 'https://maps.app.goo.gl/LYzAUwsB1q96ezyW9'
        when STELE_NUMBER = 8 then 'https://maps.app.goo.gl/n6UheShuqSdnuKe5A'
        when STELE_NUMBER = 9 then 'https://maps.app.goo.gl/rYsN7oQFEUJPZJx78'
        when STELE_NUMBER = 10 then 'https://maps.app.goo.gl/JvUqUwJZB4Pt6sUn8'
        when STELE_NUMBER = 11 then 'https://maps.app.goo.gl/cHr58Gn6NjGQ8aFm9'
        when STELE_NUMBER = 12 then 'https://maps.app.goo.gl/a4DxhMZb8Aggyft87'
        when STELE_NUMBER = 13 then 'https://maps.app.goo.gl/gR2xtfzBWgSWCSJt8'
        when STELE_NUMBER = 14 then 'https://maps.app.goo.gl/jdrDRNXr4sEyNEPx7'
        when STELE_NUMBER = 15 then 'https://maps.app.goo.gl/Y4khWBGTLHUsibRT7'
        when STELE_NUMBER = 16 then 'https://maps.app.goo.gl/wbwDk6fie8oHLaeZ8'
        when STELE_NUMBER = 17 then 'https://maps.app.goo.gl/EXDgZjLW73x4AZUv6'
        when STELE_NUMBER = 18 then 'https://maps.app.goo.gl/1mvNmunvZF1tjnxq5'
        when STELE_NUMBER = 19 then 'https://maps.app.goo.gl/m7abiry3Fm8jyFraA'
        when STELE_NUMBER = 20 then 'https://maps.app.goo.gl/u9JSLSzoPsd8xk8U7'
        when STELE_NUMBER = 21 then 'https://maps.app.goo.gl/2vZUbi44kg9oEjy97'
        when STELE_NUMBER = 22 then 'https://maps.app.goo.gl/BTReqVNZLiv79PNR7'
        when STELE_NUMBER = 23 then 'https://maps.app.goo.gl/76a1N7FHa2n84SxKA'
        when STELE_NUMBER = 24 then 'https://maps.app.goo.gl/1L1KnVxS4DkLhn2A6'
        when STELE_NUMBER = 25 then 'https://maps.app.goo.gl/EFBimg8x3htcq2RQ8'
        when STELE_NUMBER = 26 then 'https://maps.app.goo.gl/1CjTBniJMh6cCS8BA'
        when STELE_NUMBER = 27 then 'https://maps.app.goo.gl/X1SxrkfrQot8TGmA8'
        when STELE_NUMBER = 28 then 'https://maps.app.goo.gl/FrupemAhg7ABv27e6'
        when STELE_NUMBER = 29 then 'https://maps.app.goo.gl/kWJWqbHDBddqexqZ9'
        when STELE_NUMBER = 30 then 'https://maps.app.goo.gl/wMTvktDLnBZPZ66o6'
        when STELE_NUMBER = 31 then 'https://maps.app.goo.gl/jvTaRmMZjYg15nrH8'
        when STELE_NUMBER = 32 then 'https://maps.app.goo.gl/4eZJuURxNqC17Ktg9'
        when STELE_NUMBER = 33 then 'https://maps.app.goo.gl/qZzC9sqA1QWQKMiA9'
        when STELE_NUMBER = 34 then 'https://maps.app.goo.gl/3uhsLfmU9FaKD28a6'
        when STELE_NUMBER = 35 then 'https://maps.app.goo.gl/7dfxtnqLx7q9LLFg6'
        when STELE_NUMBER = 36 then 'https://maps.app.goo.gl/FLmhcTAajn5S2hsg9'
        when STELE_NUMBER = 37 then 'https://maps.app.goo.gl/GbYmWoQZUCDDnaYz7'
        when STELE_NUMBER = 38 then 'https://maps.app.goo.gl/BHT2FMDNpw7HpYwQ9'
        when STELE_NUMBER = 39 then 'https://maps.app.goo.gl/ioWsHy3CBkE9sgXG7'
        when STELE_NUMBER = 40 then 'https://maps.app.goo.gl/KTrD4Cmkk6BCD3Xy6'
        when STELE_NUMBER = 41 then 'https://maps.app.goo.gl/8XfSA42tX9oWEGnR8'
        when STELE_NUMBER = 42 then 'https://maps.app.goo.gl/mZqhgMknN8aD5u2X6'
        when STELE_NUMBER = 43 then 'https://maps.app.goo.gl/ZHBRzcUhQmqbBAi66'
        when STELE_NUMBER = 44 then 'https://maps.app.goo.gl/3eivr5zGf4scbXFg9'
        when STELE_NUMBER = 45 then 'https://maps.app.goo.gl/3RuK7bxYzinPSQnE6'
        when STELE_NUMBER = 46 then 'https://maps.app.goo.gl/eWG8CPioc8MzS9G66'
        else "" end,
            
        STELE_COORD = case 
        when STELE_NUMBER = 1 then '56°49\\'41.3"N 60°25\\'50.5"E'
        when STELE_NUMBER = 2 then '56°49\\'41.1"N 60°25\\'50.4"E'
        when STELE_NUMBER = 3 then '56°49\\'40.9"N 60°25\\'50.5"E'
        when STELE_NUMBER = 4 then '56°49\\'40.9"N 60°25\\'50.9"E'
        when STELE_NUMBER = 5 then '56°49\\'40.8"N 60°25\\'51.6"E'
        when STELE_NUMBER = 6 then '56°49\\'41.0"N 60°25\\'52.0"E'
        when STELE_NUMBER = 7 then '56°49\\'41.3"N 60°25\\'52.1"E'
        when STELE_NUMBER = 8 then '56°49\\'41.6"N 60°25\\'52.0"E'
        when STELE_NUMBER = 9 then '56°49\\'41.8"N 60°25\\'51.5"E'
        when STELE_NUMBER = 10 then '56°49\\'42.0"N 60°25\\'51.2"E'
        when STELE_NUMBER = 11 then '56°49\\'41.8"N 60°25\\'52.4"E'
        when STELE_NUMBER = 12 then '56°49\\'41.9"N 60°25\\'52.8"E'
        when STELE_NUMBER = 13 then '56°49\\'42.2"N 60°25\\'53.4"E'
        when STELE_NUMBER = 14 then '56°49\\'42.5"N 60°25\\'53.7"E'
        when STELE_NUMBER = 15 then '56°49\\'42.6"N 60°25\\'54.0"E'
        when STELE_NUMBER = 16 then '56°49\\'42.9"N 60°25\\'53.7"E'
        when STELE_NUMBER = 17 then '56°49\\'42.8"N 60°25\\'53.4"E'
        when STELE_NUMBER = 18 then '56°49\\'42.8"N 60°25\\'53.1"E'
        when STELE_NUMBER = 19 then '56°49\\'42.7"N 60°25\\'52.5"E'
        when STELE_NUMBER = 20 then '56°49\\'42.8"N 60°25\\'51.6"E'
        when STELE_NUMBER = 21 then '56°49\\'42.9"N 60°25\\'52.2"E'
        when STELE_NUMBER = 22 then '56°49\\'43.2"N 60°25\\'53.0"E'
        when STELE_NUMBER = 23 then '56°49\\'43.1"N 60°25\\'52.3"E'
        when STELE_NUMBER = 24 then '56°49\\'43.3"N 60°25\\'52.5"E'
        when STELE_NUMBER = 25 then '56°49\\'43.4"N 60°25\\'51.7"E'
        when STELE_NUMBER = 26 then '56°49\\'43.5"N 60°25\\'52.1"E'
        when STELE_NUMBER = 27 then '56°49\\'43.8"N 60°25\\'51.1"E'
        when STELE_NUMBER = 28 then '56°49\\'43.9"N 60°25\\'50.6"E'
        when STELE_NUMBER = 29 then '56°49\\'43.5"N 60°25\\'50.8"E'
        when STELE_NUMBER = 30 then '56°49\\'43.7"N 60°25\\'50.5"E'
        when STELE_NUMBER = 31 then '56°49\\'43.9"N 60°25\\'49.7"E'
        when STELE_NUMBER = 32 then '56°49\\'43.7"N 60°25\\'49.8"E'
        when STELE_NUMBER = 33 then '56°49\\'43.7"N 60°25\\'48.7"E'
        when STELE_NUMBER = 34 then '56°49\\'43.7"N 60°25\\'48.2"E'
        when STELE_NUMBER = 35 then '56°49\\'43.4"N 60°25\\'47.6"E'
        when STELE_NUMBER = 36 then '56°49\\'43.2"N 60°25\\'47.2"E'
        when STELE_NUMBER = 37 then '56°49\\'42.7"N 60°25\\'47.2"E'
        when STELE_NUMBER = 38 then '56°49\\'42.3"N 60°25\\'47.7"E'
        when STELE_NUMBER = 39 then '56°49\\'41.9"N 60°25\\'48.4"E'
        when STELE_NUMBER = 40 then '56°49\\'41.9"N 60°25\\'48.9"E'
        when STELE_NUMBER = 41 then '56°49\\'41.5"N 60°25\\'49.1"E'
        when STELE_NUMBER = 42 then '56°49\\'41.2"N 60°25\\'49.3"E'
        when STELE_NUMBER = 43 then '56°49\\'41.0"N 60°25\\'49.5"E'
        when STELE_NUMBER = 44 then '56°49\\'41.0"N 60°25\\'49.9"E'
        when STELE_NUMBER = 45 then '56°49\\'41.1"N 60°25\\'50.0"E'
        when STELE_NUMBER = 46 then '56°49\\'41.4"N 60°25\\'50.1"E'
        else "" end;
        """
    )
    )

    connection.commit()