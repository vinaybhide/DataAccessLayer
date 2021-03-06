select SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, max(NAVRECORDS.NAVDATE), NAVRECORDS.NET_ASSET_VALUE
from NAVRECORDS
INNER JOIN SCHEMES ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE
GROUP BY NAVRECORDS.SCHEMECODE
 
SELECT NAVRECORDS.ROWID AS ID, SCHEME_TYPE.ROWID as SCHEMETYPEID, SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, SCHEMES.SCHEMECODE, 
SCHEMES.SCHEMENAME, NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDATE  
from SCHEMES 
INNER JOIN SCHEME_TYPE ON SCHEMES.SCHEMETYPEID = SCHEME_TYPE.ROWID 
INNER JOIN FUNDHOUSE ON SCHEMES.FUNDHOUSECODE = FUNDHOUSE.FUNDHOUSECODE 
INNER JOIN NAVRECORDS ON SCHEMES.SCHEMECODE = NAVRECORDS.SCHEMECODE 
WHERE SCHEMES.SCHEMECODE = 144333 AND date(NAVRECORDS.NAVDATE) >= date("2021-10-21") AND date(NAVRECORDS.NAVDATE) <= date("2021-10-21") 
 
