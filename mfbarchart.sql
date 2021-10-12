SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.NAME AS FundHouse, SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, 
								min(strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE)) as FirstPurchaseDate,
								sum(PORTFOLIO.PURCHASE_UNITS) as CumulativeUnits, 
                               sum(PORTFOLIO.VALUE_AT_COST) as CumulativeCost, 
							   NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, 
							   strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate,
							   (sum(PORTFOLIO.PURCHASE_UNITS) * NAVRECORDS.NET_ASSET_VALUE) as CumulativeValue
							   from SCHEMES 
                               INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE 
                               INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE 
                               INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE 
                               WHERE NAVRECORDS.NAVDATE = SCHEMES.TO_DATE AND PORTFOLIO.MASTER_ROWID = 2
							   GROUP BY FundName