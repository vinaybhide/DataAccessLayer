using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace DataAccessLayer
{
    public class DataManager
    {

        //webservice_url = "https://www.amfiindia.com/spages/NAVAll.txt?t=27092020012930"; //string.Format(mfCurrentNAVALL_URL);
        //webservice_url = "https://www.amfiindia.com/spages/NAVAll.txt?t=27-09-2020"; //string.Format(mfCurrentNAVALL_URL);
        // string urlMF_MASTER_CURRENT = "https://www.amfiindia.com/spages/NAVAll.txt?t={0}";


        //Following URL will fetch latest NAV for ALL MF in following format
        //Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
        static string urlMF_MASTER_CURRENT = "https://www.amfiindia.com/spages/NAVAll.txt";


        //Use following URL to get specific date NAV for ALL MF. The format is same as urlMF_MASTER_CURRENT
        //Output is:
        //Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date

        //http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Jan-2020
        static string urlMF_NAV_FOR_DATE = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={0}";


        //Use following URL to get NAV history between from dt & to dt for specific MF code. 
        //Output is :
        //Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
        //http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=27&frmdt=27-Sep-2020&todt=05-Oct-2020
        static string urlMF_NAV_HISTORY_FROM_TO = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&frmdt={1}&todt={2}";
        static string urlMF_NAV_HISTORY_FROM = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&frmdt={1}";

        //Use folowwing URT to get NAV for FUNDHOUSE CODE, SCHEMETYEPE (1= open ended, 2 = Close Ended, 3 = Interval funds, From Date is mandatory, TO date is optional
        //Output is - Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
        static string urlMF_TP_NAV_HISTORY_FROM_TO = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp={1}&frmdt={2}&todt={3}";
        static string urlMF_TP_NAV_HISTORY_FROM = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&tp={1}&frmdt={2}";

        //static string dbFile = ".\\portfolio\\MFData\\mfdata.db";
        static string dbFile = "mfdata.db";
        //static void Main(string[] args)
        //{
        //    dbFile = args[0];
        //    //getAllMFNAVToday();
        //    TestLoadFromTo();
        //    //Console.WriteLine("Hello World!");
        //    //SQLiteConnection sqlite_conn;
        //    //sqlite_conn = CreateConnection();
        //    ////CreateTable(sqlite_conn);
        //    ////InsertData(sqlite_conn);
        //    //ReadData(sqlite_conn);
        //}

        public SQLiteConnection CreateConnection()
        {
            SQLiteConnection sqlite_conn = null;

            string sCurrentDir = AppDomain.CurrentDomain.BaseDirectory;
            string sFile = System.IO.Path.Combine(sCurrentDir, dbFile);
            //string sFilePath = Path.GetFullPath(sFile);
            string sFilePath = Path.GetPathRoot(sFile) + @"ProdDatabase\" + dbFile;

            // Create a new database connection:
            //sqlite_conn = new SQLiteConnection(@"Data Source= E:\MSFT_SampleWork\Analytics\portfolio\MFData\mfdata.db; " +
            //    "   Version = 3; FailIfMissing=True; Foreign Keys=True; New = True; Compress = True; ");

            sqlite_conn = new SQLiteConnection("Data Source=" + sFilePath +
                ";   Version = 3; FailIfMissing=True; Foreign Keys=True; New = True; Compress = True; PRAGMA synchronous=OFF;");

            // Open the connection:
            try
            {
                sqlite_conn.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw (ex);
            }
            return sqlite_conn;
        }

        public void ReadData(SQLiteConnection conn)
        {
            SQLiteDataReader sqlite_datareader;
            SQLiteCommand sqlite_cmd;
            sqlite_cmd = conn.CreateCommand();
            sqlite_cmd.CommandText = "SELECT * FROM FUNDHOUSE";

            sqlite_datareader = sqlite_cmd.ExecuteReader();
            while (sqlite_datareader.Read())
            {
                int fundhousecode = Int32.Parse(sqlite_datareader[0].ToString());
                string name = sqlite_datareader[1].ToString();
                Console.WriteLine("code = " + fundhousecode + ", name = " + name);
            }
            conn.Close();
        }

        #region fetch from web methods

        /// <summary>
        /// Method to fetch LATEST NAV for all MF's for all MF companies and all MF types
        /// </summary>
        /// <returns>true if data is fetched & processed successfully else false</returns>
        public bool getAllMFNAVToday()
        {
            string webservice_url;
            Uri url;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            StringBuilder sourceFile;
            long recCounter = 0;
            try
            {
                //https://www.amfiindia.com/spages/NAVAll.txt;
                //webservice_url = string.Format(urlMF_MASTER_CURRENT, DateTime.Today.Date.ToShortDateString());
                webservice_url = urlMF_MASTER_CURRENT;
                url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = WebRequestMethods.File.DownloadFile;
                //webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                if (reader != null)
                {
                    sourceFile = new StringBuilder(reader.ReadToEnd());

                    if (reader != null)
                        reader.Close();
                    if (receiveStream != null)
                        receiveStream.Close();

                    recCounter = insertRecordInDB(sourceFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Method - getAllMFNAVToday: " + "record processing failed with following error");
                Console.WriteLine(ex.Message);
                return false;
            }
            Console.WriteLine("Method - getAllMFNAVToday: " + recCounter + "records processed successfully");
            return true;
        }

        /// <summary>
        /// //This method will fetch MF data for specific date with following URL.
        ///http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Jan-2020
        ///The output is in different format than NAVALL for the current NAV
        ///The out put of this URL is as below
        ///Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date

        ///output of the method is table in following format
        ///MF_TYPE;MF_COMP_NAME;SCHEME_CODE;ISIN_Div_Payout_ISIN_Growth;ISIN_Div_Reinvestment;SCHEME_NAME;NET_ASSET_VALUE;DATE
        /// </summary>
        /// <param name="fetchDate"></param>
        /// <returns></returns>
        public bool getAllMFNAVForDate(string fetchDate)
        {
            string webservice_url;
            Uri url;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            StringBuilder sourceFile;
            long recCounter = 0;
            string dateFetch;
            try
            {
                dateFetch = System.Convert.ToDateTime(fetchDate).ToString("yyyy-MM-dd");

                //http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Jan-2020
                //webservice_url = string.Format(urlMF_MASTER_CURRENT, DateTime.Today.Date.ToShortDateString());
                webservice_url = string.Format(urlMF_NAV_FOR_DATE, dateFetch);
                url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = WebRequestMethods.File.DownloadFile;
                //webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                if (reader != null)
                {
                    sourceFile = new StringBuilder(reader.ReadToEnd());

                    if (reader != null)
                        reader.Close();
                    if (receiveStream != null)
                        receiveStream.Close();

                    recCounter = insertRecordInDB(sourceFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Method - getAllMFNAVForDate: " + "record processing failed with following error");
                Console.WriteLine(ex.Message);
                return false;
            }
            Console.WriteLine("Method - getAllMFNAVForDate: " + recCounter + "records processed successfully");
            return true;
        }

        /// <summary>
        /// //This method will fetch MF NAV history data for specific MF Code between from date = fromDt & To date < to date
        ///http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=27&frmdt=2020-09-01&todt=2020-09-04
        ///The output is in different format than NAVALL for the current NAV
        ///The out put of this URL is as below
        ///Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
        ///output of the method is table in following format
        ///MF_TYPE;MF_COMP_NAME;SCHEME_CODE;ISIN_Div_Payout_ISIN_Growth;ISIN_Div_Reinvestment;SCHEME_NAME;NET_ASSET_VALUE;DATE
        /// </summary>
        /// <param name="mfCode">Code of MF company</param>
        /// <param name="fromdt">From date string in yyyy-MM-dd format</param>
        /// <param name="todt">optional TO date string in yyyy-MM-dd format</param>
        /// <returns>true if ALL records processed successfully else false even if partial success</returns>
        public bool getHistoryNAVForMFCode(string mfCode, string fromdt, string todt = null)
        {
            string webservice_url;
            Uri url;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            StringBuilder sourceFile;
            long recCounter = 0;
            DateTime dateFrom;
            DateTime dateTo; ;
            try
            {

                dateFrom = System.Convert.ToDateTime(fromdt);//System.Convert.ToDateTime(fromdt).ToString("yyyy-MM-dd");

                if (todt != null)
                {
                    dateTo = System.Convert.ToDateTime(todt);//System.Convert.ToDateTime(todt).ToString("yyyy-MM-dd");
                    webservice_url = string.Format(urlMF_NAV_HISTORY_FROM_TO, mfCode, dateFrom.ToString("yyyy-MM-dd"), dateTo.ToString("yyyy-MM-dd"));
                }
                else
                {
                    webservice_url = string.Format(urlMF_NAV_HISTORY_FROM, mfCode, dateFrom.ToString("yyyy-MM-dd"));
                }
                url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = WebRequestMethods.File.DownloadFile;
                //webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                if (reader != null)
                {
                    sourceFile = new StringBuilder(reader.ReadToEnd());

                    if (reader != null)
                        reader.Close();
                    if (receiveStream != null)
                        receiveStream.Close();

                    recCounter = insertRecordInDB(sourceFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Method - getHistoryNAVForMFCode: " + "record processing failed with following error");
                Console.WriteLine(ex.Message);
                return false;
            }
            Console.WriteLine("Method - getHistoryNAVForMFCode: " + recCounter + "records processed successfully");
            return true;
        }

        public bool refreshNAVForMFCodeMFType(string mfCode, string mfType, string fromdt, string todt = null)
        {
            string webservice_url;
            Uri url;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            StringBuilder sourceFile;
            long recCounter = 0;
            DateTime dateFrom;
            DateTime dateTo; ;

            try
            {

                dateFrom = System.Convert.ToDateTime(fromdt);//System.Convert.ToDateTime(fromdt).ToString("yyyy-MM-dd");

                if (todt != null)
                {
                    dateTo = System.Convert.ToDateTime(todt);//System.Convert.ToDateTime(todt).ToString("yyyy-MM-dd");
                    webservice_url = string.Format(urlMF_TP_NAV_HISTORY_FROM_TO, mfCode, mfType, dateFrom.ToString("yyyy-MM-dd"), dateTo.ToString("yyyy-MM-dd"));
                }
                else
                {
                    webservice_url = string.Format(urlMF_TP_NAV_HISTORY_FROM, mfCode, mfType, dateFrom.ToString("yyyy-MM-dd"));
                }
                url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = WebRequestMethods.File.DownloadFile;
                //webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                if (reader != null)
                {
                    sourceFile = new StringBuilder(reader.ReadToEnd());

                    if (reader != null)
                        reader.Close();
                    if (receiveStream != null)
                        receiveStream.Close();

                    recCounter = insertRecordInDB(sourceFile);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Method - refreshNAVForMFCodeMFType: " + "record processing failed with following error");
                Console.WriteLine(ex.Message);
                return false;
            }
            Console.WriteLine("Method - refreshNAVForMFCodeMFType: " + recCounter + "records processed successfully");
            return true;
        }


        //public bool refreshFromLastUpdate(int fundhousecode, int schemetypeid, int historyperiod = 0)
        //{
        //    bool breturn = true;
        //    DataTable returnTable = null;
        //    SQLiteConnection sqlite_conn = null;
        //    SQLiteDataReader sqlite_datareader = null; ;
        //    SQLiteCommand sqlite_cmd = null;
        //    SQLiteTransaction transaction = null;

        //    string statement = "select FUNDHOUSE.FUNDHOUSECODE AS FUNDCODE, SCHEME_TYPE.TYPE_ID AS TYPEID, min(SCHEMES.TO_DATE) AS LASTUPDTDT, " +
        //        "min(SCHEMES.LAST_FETCH_DATE) as LAST_FETCH_DATE from SCHEMES " +
        //        "INNER JOIN SCHEME_TYPE ON SCHEMES.SCHEMETYPEID = SCHEME_TYPE.ROWID " +
        //        "INNER JOIN FUNDHOUSE ON SCHEMES.FUNDHOUSECODE = FUNDHOUSE.FUNDHOUSECODE " +
        //        "WHERE FUNDCODE = " + fundhousecode + " AND TYPEID = " + schemetypeid;

        //    try
        //    {
        //        try
        //        {
        //            sqlite_conn = CreateConnection();
        //            sqlite_cmd = sqlite_conn.CreateCommand();
        //            transaction = sqlite_conn.BeginTransaction();
        //            sqlite_cmd.CommandText = statement;
        //            sqlite_datareader = sqlite_cmd.ExecuteReader();
        //            returnTable = new DataTable();
        //            returnTable.Load(sqlite_datareader);
        //        }
        //        catch (SQLiteException exSQL)
        //        {
        //            Console.WriteLine("refreshFromLastUpdate: " + exSQL.Message);
        //            if (returnTable != null)
        //            {
        //                returnTable.Clear();
        //                returnTable.Dispose();
        //                returnTable = null;
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine("refreshFromLastUpdate: " + ex.Message);
        //            if (returnTable != null)
        //            {
        //                returnTable.Clear();
        //                returnTable.Dispose();
        //                returnTable = null;
        //            }
        //        }
        //        finally
        //        {
        //            if (transaction != null)
        //            {
        //                transaction.Commit();
        //                transaction.Dispose();
        //                transaction = null;
        //            }
        //            if (sqlite_datareader != null)
        //            {
        //                sqlite_datareader.Close();
        //            }
        //            if (sqlite_cmd != null)
        //            {
        //                sqlite_cmd.Dispose();
        //            }

        //            if (sqlite_conn != null)
        //            {
        //                sqlite_conn.Close();
        //                sqlite_conn.Dispose();
        //            }
        //            sqlite_conn = null;
        //            sqlite_datareader = null;
        //            sqlite_cmd = null;
        //            transaction = null;
        //        }
        //        if ((returnTable != null) && (returnTable.Rows.Count > 0))
        //        {
        //            string sLastFetchDate = returnTable.Rows[0]["LAST_FETCH_DATE"].ToString();
        //            DateTime dateLastFetch = DateTime.MinValue;
        //            if ((sLastFetchDate == null) || (sLastFetchDate == string.Empty))
        //            {
        //                dateLastFetch = System.Convert.ToDateTime(returnTable.Rows[0]["LASTUPDTDT"].ToString());
        //            }
        //            else
        //            {
        //                dateLastFetch = System.Convert.ToDateTime(sLastFetchDate);
        //            }

        //            int compare = dateLastFetch.CompareTo(DateTime.Today);
        //            //if compare == 0 do nothing; if compare < 0 then lastfetch date is older than today and hence we should fetch
        //            if (compare < 0)
        //            {
        //                //DateTime dateHistory = System.Convert.ToDateTime(returnTable.Rows[0]["LASTUPDTDT"].ToString());
        //                //if (historyperiod > 0)
        //                //{
        //                //    dateHistory = dateHistory.AddDays(-historyperiod);
        //                //}
        //                //if (dateHistory.CompareTo(DateTime.Today) < 0)
        //                //{
        //                //    //download and insert nav data as LASTUPDTDT < today
        //                //    breturn = refreshNAVForMFCodeMFType(fundhousecode.ToString(), schemetypeid.ToString(), dateHistory.ToString("yyyy-MM-dd"), DateTime.Today.ToString("yyyy-MM-dd"));
        //                //}
        //                breturn = refreshNAVForMFCodeMFType(fundhousecode.ToString(), schemetypeid.ToString(), dateLastFetch.ToString("yyyy-MM-dd"), DateTime.Today.ToString("yyyy-MM-dd"));
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("refreshFromLastUpdate: " + ex.Message);
        //    }
        //    finally
        //    {
        //        if (returnTable != null)
        //        {
        //            returnTable.Clear();
        //            returnTable.Dispose();
        //        }
        //    }
        //    return breturn;
        //}

        //public bool updateNAVForSchemesForPeriod(int schemeCode, string fromDate, string toDate, int historyperiod = 0)
        //{
        //    bool breturn = true;
        //    DataTable resultDataTable = null;
        //    SQLiteConnection sqlite_conn = null;
        //    SQLiteDataReader sqlite_datareader = null; ;
        //    SQLiteCommand sqlite_cmd = null;
        //    SQLiteTransaction transaction = null;
        //    string statement = "SELECT FUNDHOUSE.FUNDHOUSECODE as FundHouseCode, FUNDHOUSE.NAME as FundHouse, SCHEME_TYPE.TYPE_ID as SchemeType, SCHEMES.FROM_DATE as FROM_DATE, " +
        //                       "SCHEMES.TO_DATE as TO_DATE, SCHEMES.LAST_FETCH_DATE as LAST_FETCH_DATE from SCHEMES " +
        //                        "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
        //                        "INNER JOIN SCHEME_TYPE ON SCHEME_TYPE.ROWID = SCHEMES.SCHEMETYPEID " +
        //                        "WHERE SCHEMES.SCHEMECODE = " + schemeCode;
        //    try
        //    {
        //        try
        //        {
        //            sqlite_conn = CreateConnection();
        //            sqlite_cmd = sqlite_conn.CreateCommand();
        //            transaction = sqlite_conn.BeginTransaction();
        //            sqlite_cmd.CommandText = statement;
        //            sqlite_datareader = sqlite_cmd.ExecuteReader();
        //            resultDataTable = new DataTable();
        //            resultDataTable.Load(sqlite_datareader);
        //        }
        //        catch (SQLiteException exSQL)
        //        {
        //            Console.WriteLine("updateNAVForSchemesForPeriod: " + exSQL.Message);
        //            if (resultDataTable != null)
        //            {
        //                resultDataTable.Clear();
        //                resultDataTable.Dispose();
        //                resultDataTable = null;
        //            }
        //            breturn = false;
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine("updateNAVForSchemesForPeriod: " + ex.Message);
        //            if (resultDataTable != null)
        //            {
        //                resultDataTable.Clear();
        //                resultDataTable.Dispose();
        //                resultDataTable = null;
        //            }
        //            breturn = false;
        //        }
        //        finally
        //        {
        //            if (transaction != null)
        //            {
        //                transaction.Commit();
        //                transaction.Dispose();
        //                transaction = null;
        //            }
        //            if (sqlite_datareader != null)
        //            {
        //                sqlite_datareader.Close();
        //            }
        //            if (sqlite_cmd != null)
        //            {
        //                sqlite_cmd.Dispose();
        //            }

        //            if (sqlite_conn != null)
        //            {
        //                sqlite_conn.Close();
        //                sqlite_conn.Dispose();
        //            }
        //            sqlite_conn = null;
        //            sqlite_datareader = null;
        //            sqlite_cmd = null;
        //            transaction = null;
        //        }

        //        if ((resultDataTable != null) && (resultDataTable.Rows.Count > 0))
        //        {
        //            //we will always try to get data from fromDate. The insert 
        //            breturn = refreshNAVForMFCodeMFType(resultDataTable.Rows[0]["FundHouseCode"].ToString(), resultDataTable.Rows[0]["SchemeType"].ToString(),
        //                                                fromDate, toDate);
        //        }

        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("updateNAVForSchemesForPeriod: " + ex.Message);
        //        if (resultDataTable != null)
        //        {
        //            resultDataTable.Clear();
        //            resultDataTable.Dispose();
        //            resultDataTable = null;
        //        }
        //        breturn = false;
        //    }
        //    finally
        //    {
        //        if (resultDataTable != null)
        //        {
        //            resultDataTable.Clear();
        //            resultDataTable.Dispose();
        //        }
        //    }

        //    return breturn;
        //}

        public bool updateNAVForPortfolio(string portfolioMasterRowId)
        {
            bool breturn = true;
            DataTable fundhouseTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            string statement = "select FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.LAST_UPDATE_DATE from FUNDHOUSE " +
                                "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
                                "INNER JOIN SCHEMES ON SCHEMES.FUNDHOUSECODE = FUNDHOUSE.FUNDHOUSECODE " +
                                "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                                " GROUP BY FUNDHOUSE.FUNDHOUSECODE";
            try
            {
                try
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                    sqlite_cmd.CommandText = statement;
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    fundhouseTable = new DataTable();
                    fundhouseTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("updateNAVForPortfolio: " + exSQL.Message);
                    if (fundhouseTable != null)
                    {
                        fundhouseTable.Clear();
                        fundhouseTable.Dispose();
                        fundhouseTable = null;
                    }
                    breturn = false;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("updateNAVForPortfolio: " + ex.Message);
                    if (fundhouseTable != null)
                    {
                        fundhouseTable.Clear();
                        fundhouseTable.Dispose();
                        fundhouseTable = null;
                    }
                    breturn = false;
                }
                finally
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                        transaction = null;
                    }
                    if (sqlite_datareader != null)
                    {
                        sqlite_datareader.Close();
                    }
                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                    }

                    if (sqlite_conn != null)
                    {
                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                    }
                    sqlite_conn = null;
                    sqlite_datareader = null;
                    sqlite_cmd = null;
                    transaction = null;
                }
                DateTime dateLastUpdate = DateTime.MinValue;
                foreach (DataRow fundRow in fundhouseTable.Rows)
                {
                    dateLastUpdate = System.Convert.ToDateTime(fundRow["LAST_UPDATE_DATE"].ToString());
                    if (dateLastUpdate.Equals(DateTime.MinValue))
                    {
                        dateLastUpdate = System.Convert.ToDateTime("2008-01-01");
                    }
                    if (dateLastUpdate < DateTime.Today)
                    {
                        getHistoryNAVForMFCode(fundRow["FUNDHOUSECODE"].ToString(), fromdt: dateLastUpdate.AddDays(1).ToString("yyyy-MM-dd"), todt: DateTime.Today.ToString("yyyy-MM-dd"));
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("updateNAVForPortfolioSchemes: " + ex.Message);
                if (fundhouseTable != null)
                {
                    fundhouseTable.Clear();
                    fundhouseTable.Dispose();
                    fundhouseTable = null;
                }
                breturn = false;
            }
            finally
            {
                if (fundhouseTable != null)
                {
                    fundhouseTable.Clear();
                    fundhouseTable.Dispose();
                }
            }

            return breturn;
        }

        #endregion

        #region insert methods

        /// <summary>
        /// method that inserts data in DB. Inserts recrods in FUNDHOUSE, SCHEME_TYPE, NAVRECORDS
        /// It receives a stringbuilder object containing data for all schemes including fundhouse & scheme types and nav date & nav for the specific date
        /// </summary>
        /// <param name="sourceFile">A string builder object containing all records read from respective URL</param>
        /// <returns>Number of recrods processed</returns>
        public long insertRecordInDB(StringBuilder sourceFile)
        {
            string[] fields;
            StringBuilder record = new StringBuilder(string.Empty);
            //DataRow r;
            StringBuilder mfType = new StringBuilder(string.Empty);
            StringBuilder tmp1 = new StringBuilder(string.Empty);
            StringBuilder mfCompName = new StringBuilder(string.Empty);
            int newfundhousecode = -1, prevFundHouseCode = -1;
            long schemetyperowid = -1;
            int newschemecode = -1, prevSchemeCode = -1;

            int recCounter = 0;
            string[] sourceLines;
            double nav;
            DateTime dateNAV = DateTime.MinValue;
            DateTime dateMaxNAV = DateTime.MinValue;
            StringBuilder newSchemeName = new StringBuilder(string.Empty), ISINDivPayoutISINGrowth = new StringBuilder(string.Empty), ISINDivReinvestment = new StringBuilder(string.Empty);
            StringBuilder netAssetValue = new StringBuilder(string.Empty), navDate = new StringBuilder(string.Empty);
            StringBuilder recFormat1 = new StringBuilder("Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date");
            StringBuilder recFormat2 = new StringBuilder("Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date");
            //DateTime dateStart = DateTime.Today, dateEnd = DateTime.Today;
            StringBuilder fieldSeparator = new StringBuilder(";");
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;



            try
            {
                //No data found on the basis of selected parameters for this report
                sourceLines = sourceFile.ToString().Split('\n');
                sourceFile.Clear();

                if ((sourceLines[0].Contains(recFormat1.ToString())) || (sourceLines[0].Contains(recFormat2.ToString())))
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    var transaction = sqlite_conn.BeginTransaction();

                    record.Clear();
                    record.Append(sourceLines[recCounter++]);

                    //Now read each line and fill the data in table. We have to skip lines which do not have ';' and hence fields will be empty
                    //while (!reader.EndOfStream)
                    while (recCounter < sourceLines.Length)
                    {
                        //record = reader.ReadLine();
                        record.Clear();
                        record.Append(sourceLines[recCounter++].Trim());

                        //record = record.Trim();

                        if (record.Length == 0)
                        {
                            continue;
                        }
                        else if (record.ToString().Contains(";") == false)
                        //else if (record.Equals(fieldSeparator) == false)
                        {
                            //case of either MF type or MF House

                            tmp1.Clear();
                            tmp1.Append(record);
                            //lets read next few lines till we find a line with either ; or no ;
                            //if we find a line with ; then it's continuation of same MF Type but
                            //while (!reader.EndOfStream)
                            while (recCounter < sourceLines.Length)
                            {
                                //record = reader.ReadLine();
                                //record = sourceLines[recCounter++];
                                record.Clear();
                                record.Append(sourceLines[recCounter++].Trim());

                                //record = record.Trim();

                                if (record.Length == 0)
                                {
                                    continue;
                                }
                                else if (record.ToString().Contains(";") == false)
                                {
                                    //we found a MF company name
                                    mfType.Clear();
                                    mfType.Append(tmp1);
                                    mfCompName.Clear();
                                    mfCompName.Append(record);
                                    tmp1.Clear();
                                    tmp1.Append(record);

                                    schemetyperowid = getSchemeTypeRowId(mfType.ToString(), sqlite_cmd);
                                    if (schemetyperowid == -1)
                                    {
                                        schemetyperowid = insertSchemeType(mfType.ToString(), sqlite_cmd);
                                    }
                                    //sCurrentFundHouse = string.Empty;
                                    //sCurrentSchemeName = string.Empty;

                                    //Console.WriteLine("Schemetype= " + mfType);
                                }
                                else if (record.ToString().Contains(";") == true)
                                {
                                    //we continue with same MF type
                                    mfCompName.Clear();
                                    mfCompName.Append(tmp1);

                                    //First check if MF COMP NAME exist in fundhouse table

                                    newfundhousecode = getFundHouseCode(mfCompName.ToString(), sqlite_cmd);
                                    if (newfundhousecode == -1)
                                    {
                                        //this should never happen as the fundhouse table is manually maintained
                                        newfundhousecode = insertFundHouse(mfCompName.ToString(), sqlite_cmd);
                                    }
                                    if ((prevFundHouseCode == -1) || (prevFundHouseCode != newfundhousecode))
                                    {
                                        //first update LAST_UPDT_DATE of fundhouse table for previous fundhouse code
                                        if ((prevFundHouseCode != -1) && (dateMaxNAV != DateTime.MinValue))
                                        {
                                            updateFundHouseLastUpdateDate(prevFundHouseCode, dateMaxNAV, sqlite_cmd);
                                        }
                                        prevFundHouseCode = newfundhousecode;
                                    }
                                    //Console.WriteLine("Fund House= " + mfCompName);

                                    break;
                                }
                            }
                        }

                        fields = record.ToString().Split(';');

                        //record can be one of following
                        //Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
                        //Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
                        //Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date

                        if ((fields.Length == 6) || (fields.Length == 8))
                        {
                            //if NetAssetValue = 0.00 then skip this record
                            netAssetValue.Clear();
                            netAssetValue.Append(fields[4]);
                            try
                            {
                                nav = System.Convert.ToDouble(netAssetValue.ToString());
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("insertRecordInDB NAV received as: " + netAssetValue.ToString() + " skipping this record due to exce[tion: " + ex.Message);
                                nav = 0.00;
                            }
                            if (nav == 0)
                            {
                                continue;
                            }
                            //first get the schemecode
                            newschemecode = int.Parse(fields[0]);
                            ISINDivPayoutISINGrowth.Clear();
                            ISINDivPayoutISINGrowth.Append(fields[1]); ;
                            ISINDivReinvestment.Clear();
                            ISINDivReinvestment.Append(fields[2]);
                            newSchemeName.Clear();
                            newSchemeName.Append(fields[3]);
                            navDate.Clear();
                            navDate.Append(fields[5]);

                            if (fields.Length == 8)
                            {
                                newSchemeName.Clear();
                                newSchemeName.Append(fields[1]);
                                ISINDivPayoutISINGrowth.Clear();
                                ISINDivPayoutISINGrowth.Append(fields[2]);
                                ISINDivReinvestment.Clear();
                                ISINDivReinvestment.Append(fields[3]);
                                navDate.Clear();
                                navDate.Append(fields[7]);
                            }

                            dateNAV = System.Convert.ToDateTime(navDate.ToString());

                            //Now check if scheme exists in SCHEMES table
                            //if (sCurrentSchemeName.Equals(schemeName) == false)
                            if ((prevSchemeCode == -1) || (prevSchemeCode != newschemecode))
                            {
                                //if (isSchemeExists(newschemecode, sqlite_cmd).ToUpper().Equals(schemeName.ToString().ToUpper()) == false)
                                if (isSchemeExists(newschemecode, sqlite_cmd) == false)
                                {
                                    //insert new scheme in schemes tables
                                    insertScheme(newfundhousecode, schemetyperowid, newschemecode, newSchemeName.ToString(), sqlite_cmd);
                                }
                                prevSchemeCode = newschemecode;
                            }
                            //MF_TYPE;MF_COMP_NAME;SCHEME_CODE;ISIN_Div_Payout_ISIN_Growth;ISIN_Div_Reinvestment;SCHEME_NAME;NET_ASSET_VALUE;DATE
                            insertTransaction(newschemecode, ISINDivPayoutISINGrowth.ToString(), ISINDivReinvestment.ToString(), newSchemeName.ToString(),
                                      string.Format("{0:0.0000}", nav), dateNAV.ToString("yyyy-MM-dd"), sqlite_cmd);

                            if (dateMaxNAV < dateNAV)
                            {
                                dateMaxNAV = dateNAV;
                            }
                        }
                    }
                    //update the last or ONLY fundhouse
                    if ((prevFundHouseCode != -1) && (dateMaxNAV != DateTime.MinValue))
                    {
                        updateFundHouseLastUpdateDate(prevFundHouseCode, dateMaxNAV, sqlite_cmd);
                    }

                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertRecordInDB exception: " + ex.Message);
                throw ex;
            }
            if (sqlite_cmd != null)
            {
                sqlite_cmd.Dispose();
            }

            if (sqlite_conn != null)
            {
                sqlite_conn.Close();
                sqlite_conn.Dispose();
            }

            return recCounter;
        }

        public long insertSchemeType(string schemeType, SQLiteCommand sqlite_cmd)
        {
            long schemetypeid = -1;
            string schemeCategory = "";
            int type_id = -1;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                if (schemeType.Contains("Open Ended Schemes"))
                {
                    schemeCategory = "Open Ended Schemes";
                    type_id = 1;
                }
                else if (schemeType.Contains("Close Ended Schemes"))
                {
                    schemeCategory = "Close Ended Schemes";
                    type_id = 2;
                }
                else if (schemeType.Contains("Interval Fund Schemes"))
                {
                    schemeCategory = "Interval Fund Schemes";
                    type_id = 3;
                }
                sqlite_cmd.CommandText = "INSERT OR IGNORE INTO  SCHEME_TYPE(TYPE, CATEGORY, TYPE_ID) VALUES (@TYPE, @CATEGORY, @TYPE_ID)";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@TYPE", schemeType);
                sqlite_cmd.Parameters.AddWithValue("@CATEGORY", schemeCategory);
                sqlite_cmd.Parameters.AddWithValue("@TYPE_ID", type_id);

                try
                {
                    schemetypeid = sqlite_cmd.ExecuteNonQuery();
                    //if (schemetypeid > 0)
                    //{
                    //    sqlite_cmd.CommandText = "SELECT seq from sqlite_sequence WHERE name = \"SCHEME_TYPE\"";
                    //    schemetypeid = Convert.ToInt64(sqlite_cmd.ExecuteScalar());
                    //    //schemetypeid = getSchemeTypeId(schemeType);
                    //}
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("insertSchemeType: [" + schemeType + "] " + exSQL.Message);
                }

                //if (sqlite_cmd != null)
                //{
                //    sqlite_cmd.Dispose();
                //}

                //if (sqlite_conn != null)
                //{
                //    sqlite_conn.Close();
                //    sqlite_conn.Dispose();
                //}

                if (schemetypeid > 0)
                {
                    schemetypeid = getSchemeTypeRowId(schemeType, sqlite_cmd);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertSchemeType: [" + schemeType + "] " + ex.Message);
            }
            //sqlite_conn = null;
            //sqlite_cmd = null;
            return schemetypeid;
        }

        public int insertFundHouse(string fundHouse, SQLiteCommand sqlite_cmd)
        {
            int fundhousecode = -1;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //first get max(fundhousecode)
                fundhousecode = getMaxFundHouseID(fundHouse, sqlite_cmd);

                if (fundhousecode != -1)
                {
                    //sqlite_conn = CreateConnection();
                    //sqlite_cmd = sqlite_conn.CreateCommand();

                    try
                    {
                        sqlite_cmd.CommandText = "INSERT OR IGNORE INTO FUNDHOUSE(FUNDHOUSECODE, NAME) VALUES (@CODE, @NAME)";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@CODE", fundhousecode);
                        sqlite_cmd.Parameters.AddWithValue("@NAME", fundHouse);
                        sqlite_cmd.ExecuteNonQuery();
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("insertFundHouse: [" + fundHouse + "] " + exSQL.Message);
                        fundhousecode = -1;
                    }
                    //if (sqlite_cmd != null)
                    //{
                    //    sqlite_cmd.Dispose();
                    //}

                    //if (sqlite_conn != null)
                    //{
                    //    sqlite_conn.Close();
                    //    sqlite_conn.Dispose();
                    //}
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertFundHouse: [" + fundHouse + "] " + ex.Message);
            }
            //sqlite_conn = null;
            //sqlite_cmd = null;
            return fundhousecode;
        }

        public int insertScheme(int fundHouseCode, long schemeTypeId, int schemeCode, string schemeName, SQLiteCommand sqlite_cmd)
        {
            int numOfRowsInserted = 0;
            try
            {
                //sqlite_cmd.CommandText = "REPLACE INTO SCHEMES(SCHEMECODE, SCHEMENAME, FUNDHOUSECODE, SCHEMETYPEID) VALUES (@SCHEMECODE, @SCHEMENAME, @FUNDHOUSECODE, @SCHEMETYPEID)";
                sqlite_cmd.CommandText = "INSERT OR IGNORE INTO SCHEMES(SCHEMECODE, SCHEMENAME, FUNDHOUSECODE, SCHEMETYPEID) " +
                                         "VALUES (@SCHEMECODE, @SCHEMENAME, @FUNDHOUSECODE, @SCHEMETYPEID)";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMENAME", schemeName);
                sqlite_cmd.Parameters.AddWithValue("@FUNDHOUSECODE", fundHouseCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMETYPEID", schemeTypeId);

                //sqlite_cmd.CommandText = "INSERT INTO SCHEMES(SCHEMECODE, SCHEMENAME, FUNDHOUSECODE, SCHEMETYPEID) VALUES ('" +
                //    schemeCode + "','" + schemeName + "'," +  fundHouseCode.ToString() + "," + schemeTypeId.ToString() + ")";

                try
                {
                    numOfRowsInserted = sqlite_cmd.ExecuteNonQuery();
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("insertScheme: [" + fundHouseCode + "," + schemeTypeId + "," + schemeCode + "," + schemeName + "] " + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertScheme: [" + fundHouseCode + "," + schemeTypeId + "," + schemeCode + "," + schemeName + "] " + ex.Message);
            }
            return numOfRowsInserted;
        }

        //Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
        //insertTransaction(fundhousecode, schemetypeid, schemecode, fields[1], fields[2], fields[3], string.Format("{0:0.0000}", nav), System.Convert.ToDateTime(fields[5]).ToString("yyyy-MM-dd"));
        public int insertTransaction(int schemeCode, string ISINDivPayout_ISINGrowth, string ISINDivReinvestment,
                                           string schemeName, string netAssetValue, string navDate, SQLiteCommand sqlite_cmd)
        {
            int numOfRowsInserted = 0;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                //sqlite_cmd.CommandText = "INSERT OR IGNORE INTO NAVRECORDS(SCHEMECODE, ISIN_Div_Payout_ISIN_Growth, ISIN_Div_Reinvestment, " +
                sqlite_cmd.CommandText = "INSERT OR IGNORE INTO NAVRECORDS(SCHEMECODE, ISIN_Div_Payout_ISIN_Growth, ISIN_Div_Reinvestment, " +
                    "NET_ASSET_VALUE, NAVDATE) VALUES (@SCHEMECODE, @ISIN_Div_Payout_ISIN_Growth, @ISIN_Div_Reinvestment, " +
                    "@NET_ASSET_VALUE, @NAVDATE)";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);

                sqlite_cmd.Parameters.AddWithValue("@ISIN_Div_Payout_ISIN_Growth", ISINDivPayout_ISINGrowth);
                sqlite_cmd.Parameters.AddWithValue("@ISIN_Div_Reinvestment", ISINDivReinvestment);
                sqlite_cmd.Parameters.AddWithValue("@NET_ASSET_VALUE", netAssetValue);
                sqlite_cmd.Parameters.AddWithValue("@NAVDATE", navDate);

                try
                {
                    numOfRowsInserted = sqlite_cmd.ExecuteNonQuery();
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("insertTransaction: [" + schemeCode + "," + ISINDivPayout_ISINGrowth + ","
                        + ISINDivReinvestment + "," + schemeName + "," + netAssetValue + "," + navDate + "] " + exSQL.Message);
                }
                //if (sqlite_cmd != null)
                //{
                //    sqlite_cmd.Dispose();
                //}

                //if (sqlite_conn != null)
                //{
                //    sqlite_conn.Close();
                //    sqlite_conn.Dispose();
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertTransaction: [" + schemeCode + "," + ISINDivPayout_ISINGrowth + ","
                        + ISINDivReinvestment + "," + schemeName + "," + netAssetValue + "," + navDate + "] " + ex.Message);
            }
            //sqlite_conn = null;
            //sqlite_cmd = null;
            return numOfRowsInserted;
        }
        #endregion

        #region update_methods

        public int updateFundHouseLastUpdateDate(int fundhousecode, DateTime lastUpdateDate, SQLiteCommand sqlite_cmd = null)
        {
            int numOfRowsUpdated = 0;
            SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            SQLiteDataReader sqlite_datareader = null;
            DateTime dateFromDB = DateTime.MinValue;
            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                //sqlite_cmd.CommandText = "UPDATE SCHEMES SET FROM_DATE = T.MINNAVDATE, TO_DATE = T.MAXNAVDATE " +
                //                         "FROM (select MIN(NAVDATE) AS MINNAVDATE, MAX(NAVDATE) AS MAXNAVDATE, SCHEMECODE from NAVRECORDS GROUP BY 3) AS T " +
                //                         "WHERE SCHEMES.SCHEMECODE = T.SCHEMECODE";

                try
                {
                    dateFromDB = getFundHouseLastUpdateDate(fundhousecode, sqlite_cmd);
                    if (dateFromDB < lastUpdateDate)
                    {
                        sqlite_cmd.CommandText = "UPDATE FUNDHOUSE SET LAST_UPDATE_DATE = '" + lastUpdateDate.ToString("yyyy-MM-dd") + "' WHERE FUNDHOUSECODE = " + fundhousecode;
                        numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("UpdateSchemeFromToDate: " + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertScheme: " + ex.Message);
            }
            finally
            {
                if (transaction != null)
                {
                    transaction.Commit();
                    transaction.Dispose();
                }
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_conn != null)
                {
                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                        sqlite_cmd = null;
                    }
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                transaction = null;
                sqlite_conn = null;
                sqlite_datareader = null;
            }
            return numOfRowsUpdated;
        }

        //public int updateSchemeFromDate(int schemeCode, string schemeName, string dateFrom)
        //{
        //    int numOfRowsUpdated = 0;
        //    SQLiteConnection sqlite_conn = null;
        //    SQLiteCommand sqlite_cmd = null;
        //    try
        //    {
        //        sqlite_conn = CreateConnection();
        //        sqlite_cmd = sqlite_conn.CreateCommand();
        //        sqlite_cmd.CommandText = "UPDATE SCHEMES SET FROM_DATE = @FROM_DATE WHERE SCHEMECODE = @SCHEMECODE AND SCHEMENAME = @SCHEMENAME";
        //        sqlite_cmd.Prepare();
        //        sqlite_cmd.Parameters.AddWithValue("@FROM_DATE", dateFrom);
        //        sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
        //        sqlite_cmd.Parameters.AddWithValue("@SCHEMENAME", schemeName);
        //        try
        //        {
        //            numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
        //        }
        //        catch (SQLiteException exSQL)
        //        {
        //            Console.WriteLine("updateSchemeFromDate: [" + schemeCode + "," + schemeName + "," + dateFrom + "] " + exSQL.Message);
        //        }
        //        if (sqlite_cmd != null)
        //        {
        //            sqlite_cmd.Dispose();
        //        }

        //        if (sqlite_conn != null)
        //        {
        //            sqlite_conn.Close();
        //            sqlite_conn.Dispose();
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("insertScheme: [" + schemeCode + "," + schemeName + "] " + ex.Message);
        //    }
        //    sqlite_conn = null;
        //    sqlite_cmd = null;
        //    return numOfRowsUpdated;
        //}

        //public int updateSchemeFromToDate(int schemeCode, string schemeName, DateTime dateFrom, DateTime dateTo, SQLiteCommand sqlite_cmd = null)
        //{
        //    int numOfRowsUpdated = 0;
        //    SQLiteConnection sqlite_conn = null;
        //    SQLiteTransaction transaction = null;
        //    try
        //    {
        //        if (sqlite_cmd == null)
        //        {
        //            sqlite_conn = CreateConnection();
        //            sqlite_cmd = sqlite_conn.CreateCommand();
        //            transaction = sqlite_conn.BeginTransaction();
        //        }

        //        //if dateTo is MinVal then we should not update the TO_DATE
        //        sqlite_cmd.CommandText = "UPDATE SCHEMES SET LAST_FETCH_DATE = '" + DateTime.Today.ToString("yyyy-MM-dd") + "'";
        //        if (dateFrom.Equals(DateTime.MinValue) == false)
        //        {
        //            //if dateFrom is minVal then we should not update the FROM_DATE
        //            sqlite_cmd.CommandText += ", FROM_DATE = '" + dateFrom.ToString("yyyy-MM-dd") + "'";
        //        }
        //        if (dateTo.Equals(DateTime.MinValue) == false)
        //        {
        //            sqlite_cmd.CommandText += ", TO_DATE = '" + dateTo.ToString("yyyy-MM-dd") + "'";
        //        }

        //        sqlite_cmd.CommandText += " WHERE SCHEMECODE = " + schemeCode + " AND SCHEMENAME = '" + schemeName + "'";

        //        try
        //        {
        //            numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
        //        }
        //        catch (SQLiteException exSQL)
        //        {
        //            Console.WriteLine("updateSchemeFromToDate: [" + schemeCode + "," + schemeName + "," + dateTo + "] " + exSQL.Message);
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("insertScheme: [" + schemeCode + "," + schemeName + "] " + ex.Message);
        //    }
        //    finally
        //    {
        //        if (sqlite_conn != null)
        //        {
        //            if (transaction != null)
        //            {
        //                transaction.Commit();
        //                transaction.Dispose();
        //            }
        //            if (sqlite_cmd != null)
        //            {
        //                sqlite_cmd.Dispose();
        //            }
        //            sqlite_conn.Close();
        //            sqlite_conn.Dispose();
        //            transaction = null;
        //            sqlite_cmd = null;
        //            sqlite_conn = null;
        //        }
        //    }
        //    return numOfRowsUpdated;
        //}

        #endregion

        #region get_methods

        public DateTime getFundHouseLastUpdateDate(int fundhousecode, SQLiteCommand sqlite_cmd = null)
        {
            DateTime dateLastUpdateDate = DateTime.MinValue;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteTransaction transaction = null;
            string statement = "SELECT LAST_UPDATE_DATE from FUNDHOUSE WHERE FUNDHOUSECODE = " + fundhousecode;
            try
            {
                try
                {
                    if (sqlite_cmd == null)
                    {
                        sqlite_conn = CreateConnection();
                        sqlite_cmd = sqlite_conn.CreateCommand();
                        transaction = sqlite_conn.BeginTransaction();
                    }
                    sqlite_cmd.CommandText = statement;
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //sqlite_datareader.HasRows;
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        dateLastUpdateDate = System.Convert.ToDateTime(sqlite_datareader["LAST_UPDATE_DATE"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getMinLastFetchDateForAll: " + exSQL.Message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("getMinLastFetchDateForAll: " + ex.Message);
                }
                finally
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }
                    if (sqlite_datareader != null)
                    {
                        sqlite_datareader.Close();
                    }
                    if (sqlite_conn != null)
                    {
                        if (sqlite_cmd != null)
                        {
                            sqlite_cmd.Dispose();
                            sqlite_cmd = null;
                        }
                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                    }
                    sqlite_conn = null;
                    sqlite_datareader = null;
                    transaction = null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseLastUpdateDate: " + ex.Message);
            }
            return dateLastUpdateDate;
        }

        /// <summary>
        /// Method to get predefined fundhouse code for given fundhouse/mf company value
        /// </summary>
        /// <param name="fundHouse">name of the fund house</param>
        /// <returns>matchng fund house code</returns>
        public int getFundHouseCode(string fundHouse, SQLiteCommand sqlite_cmd = null)
        {
            int fundhousecode = -1;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            //SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                sqlite_cmd.CommandText = "SELECT FUNDHOUSECODE FROM FUNDHOUSE WHERE NAME = '" + fundHouse + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        fundhousecode = Int32.Parse(sqlite_datareader["FUNDHOUSECODE"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getFundHouseCode: [" + fundHouse + "] :" + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseCode: [" + fundHouse + "] :" + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                    sqlite_datareader = null;
                }
                if (sqlite_conn != null)
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }
                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                    }
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                    transaction = null;
                    sqlite_cmd = null;
                    sqlite_conn = null;
                }
            }
            return fundhousecode;
        }

        /// <summary>
        /// Returns all data from fundhouse table. 
        /// Columns - FUNDHOUSECODE, NAME
        /// </summary>
        /// <returns></returns>
        public DataTable getFundHouseTable(int fundhousecode = -1)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                if (fundhousecode == -1)
                {
                    sqlite_cmd.CommandText = "SELECT FUNDHOUSECODE, NAME, LAST_UPDATE_DATE FROM FUNDHOUSE";
                }
                else
                {
                    sqlite_cmd.CommandText = "SELECT FUNDHOUSECODE, NAME, LAST_UPDATE_DATE FROM FUNDHOUSE WHERE FUNDHOUSECODE = " + fundhousecode;
                }
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getFundHouseTable: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseTable: " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return returnTable;
        }
        /// <summary>
        /// Gets ID of scheme type given scheme type
        /// </summary>
        /// <param name="schemeType"></param>
        /// <returns>ID of matching scheme type</returns>
        public long getSchemeTypeRowId(string schemeType, SQLiteCommand sqlite_cmd)
        {
            long schemetypeid = -1;
            //SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "SELECT ROWID FROM SCHEME_TYPE WHERE TYPE = '" + schemeType + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        schemetypeid = Int64.Parse(sqlite_datareader["ROWID"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getSchemeTypeId: [" + schemeType + "]" + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSchemeTypeId: [" + schemeType + "]" + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                //if (sqlite_cmd != null)
                //{
                //    sqlite_cmd.Dispose();
                //}

                //if (sqlite_conn != null)
                //{
                //    sqlite_conn.Close();
                //    sqlite_conn.Dispose();
                //}

                //sqlite_conn = null;
                sqlite_datareader = null; ;
                //sqlite_cmd = null;
            }
            return schemetypeid;
        }

        /// <summary>
        /// Return all data from scheme_type table
        /// columns - ID, TYPE
        /// </summary>
        /// <returns></returns>
        public DataTable getSchemeTypeTable()
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var trancation = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = "SELECT ROWID, TYPE, CATEGORY, TYPE_ID FROM SCHEME_TYPE";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getSchemeTypeTable: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
                trancation.Commit();
                trancation.Dispose();
                trancation = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSchemeTypeTable: " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return returnTable;
        }

        /// <summary>
        /// Finds if given scheme code exists in Schemes table
        /// </summary>
        /// <param name="schemeCode"></param>
        /// <returns>matching scheme name if found else empty </returns>
        public bool isSchemeExists(int schemeCode, SQLiteCommand sqlite_cmd)
        {
            string schemename = string.Empty;
            bool breturn = false;
            //SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "SELECT SCHEMENAME FROM SCHEMES WHERE SCHEMECODE = @CODE";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@CODE", schemeCode);
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    breturn = sqlite_datareader.HasRows;
                    //if (sqlite_datareader.HasRows)
                    //if (sqlite_datareader.Read())
                    //{
                    //    //sqlite_datareader.Read();
                    //    schemename = sqlite_datareader["SCHEMENAME"].ToString();
                    //}
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("isSchemeExists: [" + schemeCode + "] " + exSQL.Message);
                    schemename = string.Empty;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("isSchemeExists: [" + schemeCode + "] " + ex.Message);
                schemename = string.Empty;
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                //if (sqlite_cmd != null)
                //{
                //    sqlite_cmd.Dispose();
                //}

                //if (sqlite_conn != null)
                //{
                //    sqlite_conn.Close();
                //    sqlite_conn.Dispose();
                //}

                //sqlite_conn = null;
                sqlite_datareader = null;
                //sqlite_cmd = null;
            }
            return breturn;
        }

        /// <summary>
        /// returns data from schemes table matching given fundhouse code and scheme type id. The filters are ignored if value passed to method = -1
        /// </summary>
        /// <param name="fundhousecode"></param>
        /// <param name="schemetypeid"></param>
        /// <returns>Data Table matching criterion provided in fundhousecode and schemetypeid</returns>
        public DataTable getSchemesTable(int fundhousecode = -1, int schemetypeid = -1)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;


            string statement = "SELECT SCHEME_TYPE.ROWID AS SCHEMETYPEID , SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME " +
                                " FROM SCHEMES " +
                                "INNER JOIN SCHEME_TYPE ON SCHEME_TYPE.ROWID = SCHEMES.SCHEMETYPEID " +
                                "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE ";

            try
            {
                if ((fundhousecode != -1) && (schemetypeid != -1))
                {
                    statement += "WHERE FUNDHOUSE.FUNDHOUSECODE = " + fundhousecode.ToString() + "AND SCHEME_TYPE.ROWID = " + schemetypeid.ToString();
                }
                else if (fundhousecode != -1)
                {
                    statement += "WHERE FUNDHOUSE.FUNDHOUSECODE = " + fundhousecode.ToString();
                }
                else if (schemetypeid != -1)
                {
                    statement += "WHERE SCHEME_TYPE.ROWID = " + schemetypeid.ToString();
                }
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getSchemesTable: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSchemesTable: " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return returnTable;
        }

        public DataTable getPortfolioSchemesTable(int portfolioMasterRowId)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;


            string statement = "SELECT SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME FROM PORTFOLIO " +
                "INNER JOIN SCHEMES ON SCHEMES.SCHEMECODE = PORTFOLIO.SCHEMECODE " +
                "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                " GROUP BY PORTFOLIO.SCHEMECODE " +
                "ORDER BY PORTFOLIO.SCHEMECODE ASC";

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getPortfolioSchemesTable: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("getPortfolioSchemesTable: " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return returnTable;
        }
        public int getMaxFundHouseID(string fundHouse, SQLiteCommand sqlite_cmd)
        {
            int fundhousecode = -1;
            //SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();

                //first get max(fundhousecode)
                sqlite_cmd.CommandText = "SELECT MAX(FUNDHOUSECODE) FROM FUNDHOUSE";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        fundhousecode = Int32.Parse(sqlite_datareader[0].ToString());
                        fundhousecode++;
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getMaxFundHouseID: [" + fundHouse + "] " + exSQL.Message);
                    fundhousecode = -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getMaxFundHouseID: [" + fundHouse + "] " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                //if (sqlite_cmd != null)
                //{
                //    sqlite_cmd.Dispose();
                //}

                //if (sqlite_conn != null)
                //{
                //    sqlite_conn.Close();
                //    sqlite_conn.Dispose();
                //}
                //sqlite_conn = null;
                sqlite_datareader = null;
                //sqlite_cmd = null;
            }
            return fundhousecode;
        }

        public DataTable getNAVRecordsTable(int schemecode, string fromDate = null, string toDate = null)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement;
            try
            {
                statement = "SELECT NAVRECORDS.ROWID AS ID, SCHEME_TYPE.ROWID as SCHEMETYPEID, SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, SCHEMES.SCHEMECODE, " +
                               "SCHEMES.SCHEMENAME, NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDATE  from SCHEMES " +
                               "INNER JOIN SCHEME_TYPE ON SCHEMES.SCHEMETYPEID = SCHEME_TYPE.ROWID " +
                               "INNER JOIN FUNDHOUSE ON SCHEMES.FUNDHOUSECODE = FUNDHOUSE.FUNDHOUSECODE " +
                               "INNER JOIN NAVRECORDS ON SCHEMES.SCHEMECODE = NAVRECORDS.SCHEMECODE ";

                statement += "WHERE SCHEMES.SCHEMECODE = " + schemecode.ToString() + " ";

                if ((fromDate.Equals(string.Empty) == false) && (fromDate != null))
                {
                    statement += "AND date(NAVRECORDS.NAVDATE) >= date(\"" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd") + "\") ";
                }

                if ((toDate.Equals(string.Empty) == false) && (toDate != null))
                {
                    statement += "AND date(NAVRECORDS.NAVDATE) <= date(\"" + System.Convert.ToDateTime(toDate).ToString("yyyy-MM-dd") + "\") ";
                }
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    returnTable = new DataTable();
                    returnTable.Columns.Add("ID", typeof(long));
                    returnTable.Columns.Add("SCHEMETYPEID", typeof(long));
                    returnTable.Columns.Add("TYPE", typeof(string));
                    returnTable.Columns.Add("FUNDHOUSECODE", typeof(int));
                    returnTable.Columns.Add("NAME", typeof(string));
                    returnTable.Columns.Add("SCHEMECODE", typeof(long));
                    returnTable.Columns.Add("SCHEMENAME", typeof(string));
                    returnTable.Columns.Add("NET_ASSET_VALUE", typeof(decimal));
                    returnTable.Columns.Add("NAVDATE", typeof(DateTime));
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable.Load(sqlite_datareader);
                    //NAVRECORDS.ID, SCHEME_TYPEs AS SCHEMETYPEID, SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, " +
                    //"SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE)  as NAVDATE
                    //returnTable.Columns["NAVDATE"].DataType = typeof(DateTime);
                    //returnTable.Columns["NET_ASSET_VALUE"].DataType = typeof(decimal);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getNAVRecordsTable: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("getNAVRecordsTable: " + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return returnTable;
        }

        #endregion

        #region portfolio
        public long createnewMFPortfolio(string userid, string portfolioname)
        {
            long portfolio_id = -1;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                try
                {
                    sqlite_cmd.CommandText = "INSERT OR IGNORE INTO   PORTFOLIO_MASTER(USERID, PORTFOLIO_NAME) VALUES (@USERID, @NAME)";
                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@USERID", userid);
                    sqlite_cmd.Parameters.AddWithValue("@NAME", portfolioname);
                    if (sqlite_cmd.ExecuteNonQuery() > 0)
                    {
                        portfolio_id = getPortfolioId(portfolioname, userid, sqlite_cmd);
                        //sqlite_cmd.CommandText = "SELECT seq from sqlite_sequence WHERE name = \"PORTFOLIO\"";
                        //portfolio_id = Convert.ToInt64(sqlite_cmd.ExecuteScalar());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("CreateNewPortfolio: " + userid + ", " + portfolioname + "\n" + exSQL.Message);
                    portfolio_id = -1;
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch
            {
                portfolio_id = -1;
            }
            finally
            {
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_cmd = null;
                sqlite_conn = null;
            }
            return portfolio_id;
        }

        public DataTable getPortfolioTable(string userId, SQLiteCommand sqlite_cmd = null)
        {
            DataTable portfolioTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            SQLiteTransaction transaction = null;
            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                sqlite_cmd.CommandText = "SELECT ROWID AS ID, USERID, PORTFOLIO_NAME FROM PORTFOLIO_MASTER WHERE USERID = '" + userId + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    portfolioTable = new DataTable();
                    portfolioTable.Columns.Add("USERID", typeof(string));
                    portfolioTable.Columns.Add("PORTFOLIO_NAME", typeof(string));
                    portfolioTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getPortfolioTable: [" + userId + "] " + exSQL.Message);
                    if (portfolioTable != null)
                    {
                        portfolioTable.Clear();
                        portfolioTable.Dispose();
                        portfolioTable = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getPortfolioTable: [" + userId + "] " + ex.Message);
                if (portfolioTable != null)
                {
                    portfolioTable.Clear();
                    portfolioTable.Dispose();
                    portfolioTable = null;
                }
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                    sqlite_datareader = null;
                }

                if (sqlite_conn != null)
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }
                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                    }
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                    transaction = null;
                    sqlite_cmd = null;
                    sqlite_conn = null;
                }
            }

            return portfolioTable;
        }
        public long getPortfolioId(string portfolioName, string userId, SQLiteCommand sqlite_cmd = null)
        {
            long portfolioId = -1;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            SQLiteTransaction transaction = null;
            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                sqlite_cmd.CommandText = "SELECT ROWID FROM PORTFOLIO_MASTER WHERE USERID = '" + userId + "' AND PORTFOLIO_NAME = '" + portfolioName + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        portfolioId = Int64.Parse(sqlite_datareader["ROWID"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getPortfoliId: [" + userId + "," + portfolioName + "]" + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getPortfoliId: [" + userId + "," + portfolioName + "]" + ex.Message);
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                    sqlite_datareader = null;
                }
                if (sqlite_conn != null)
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }
                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                    }
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                    transaction = null;
                    sqlite_cmd = null;
                    sqlite_conn = null;
                }
                sqlite_datareader = null; ;
            }
            return portfolioId;
        }

        public bool addNewTransaction(string userId, string portfolioName, string schemeCode, string purchaseDate, string purchaseNAV,
                                            string purchaseUnits, string valueAtCost, long portfolioId, SQLiteCommand sqlite_cmd = null)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            //long portfolioId;
            SQLiteTransaction transaction = null;

            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                try
                {
                    //if (portfolioId == -1)
                    //{
                    //    portfolioId = getPortfolioId(portfolioName, userId, sqlite_cmd);
                    //}
                    //if (portfolioId > 0)
                    {
                        sqlite_cmd.CommandText = "INSERT OR IGNORE INTO  PORTFOLIO(MASTER_ROWID, SCHEMECODE, PURCHASE_DATE, PURCHASE_NAV, PURCHASE_UNITS, VALUE_AT_COST) " +
                                                 "VALUES (@MASTER_ROWID, @SCHEMECODE, @PURCHASE_DATE, @PURCHASE_NAV, @PURCHASE_UNITS, @VALUE_AT_COST)";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", portfolioId);
                        sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                        sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(purchaseDate).ToString("yyyy-MM-dd"));
                        sqlite_cmd.Parameters.AddWithValue("@PURCHASE_NAV", string.Format("{0:0:0000}", purchaseNAV));
                        sqlite_cmd.Parameters.AddWithValue("@PURCHASE_UNITS", string.Format("{0:0.0000}", purchaseUnits));
                        sqlite_cmd.Parameters.AddWithValue("@VALUE_AT_COST", string.Format("{0:0.0000}", valueAtCost));
                        try
                        {
                            if (sqlite_cmd.ExecuteNonQuery() > 0)
                            {
                                breturn = true;
                            }
                        }
                        catch (SQLiteException exSQL)
                        {
                            Console.WriteLine("addNewTransaction: [" + schemeCode + "," + portfolioName + "," + purchaseDate + "," + purchaseUnits + purchaseNAV + "] " + exSQL.Message);
                        }
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("addNewTransaction: [" + schemeCode + "," + portfolioName + "," + purchaseDate + "," + purchaseUnits + purchaseNAV + "] " + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("addNewTransaction: [" + schemeCode + "," + portfolioName + "," + purchaseDate + "," + purchaseUnits + purchaseNAV + "] " + ex.Message);
            }
            finally
            {
                if (sqlite_conn != null)
                {
                    if (transaction != null)
                    {
                        transaction.Commit();
                        transaction.Dispose();
                    }

                    if (sqlite_cmd != null)
                    {
                        sqlite_cmd.Dispose();
                    }
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                    transaction = null;
                    sqlite_cmd = null;
                    sqlite_conn = null;
                }
            }
            return breturn;
        }

        public DateTime getNextSIPDate(DateTime sourcedt, int dayofmonth, string frequency)
        {
            DateTime returnDt;// = new DateTime(sourcedt.AddMonths(1).Year, sourcedt.AddMonths(1).Month, 1);

            if ((frequency.Equals("Daily")) || (frequency.Equals("Weekly")))
            {
                returnDt = sourcedt.AddDays(dayofmonth);
            }
            else
            {
                returnDt = new DateTime(sourcedt.AddMonths(1).Year, sourcedt.AddMonths(1).Month, 1);
                returnDt = returnDt.AddDays(dayofmonth - 1);
            }

            while ((returnDt.DayOfWeek == DayOfWeek.Saturday) || (returnDt.DayOfWeek == DayOfWeek.Sunday))
            {
                returnDt = returnDt.AddDays(1);
            }
            return returnDt;
        }

        public int getNextSIPDurationCounter(string frequency)
        {
            int returnCounter = 0;

            if (frequency.Equals("Daily"))
            {
                returnCounter = 1;
            }
            else if (frequency.Equals("Weekly"))
            {
                returnCounter = 7;
            }
            else
            {
                returnCounter = 30;

            }

            return returnCounter;
        }

        public bool addNewSIP(string userId, string portfolioName, long portfolioRowId, string schemeCode, string startDate, string endDate, string monthlyContribution,
                                    string sipFrequency = null, string monthday = null)
        {
            bool breturn = false;
            DataTable datewiseData = null;
            DateTime fromDt;
            DateTime toDt;
            double purchaseUnits;
            double valueAtCost = System.Convert.ToDouble(monthlyContribution);
            double purchaseNAV;
            //long portfolioId = -1;
            DateTime transDt;

            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                fromDt = System.Convert.ToDateTime(startDate);
                toDt = System.Convert.ToDateTime(endDate);

                datewiseData = getNAVRecordsTable(System.Convert.ToInt32(schemeCode), fromDate: fromDt.ToString("yyyy-MM-dd"), toDate: toDt.ToString("yyyy-MM-dd"));

                //returnTable.Columns.Add("ID", typeof(long));
                //returnTable.Columns.Add("SCHEMETYPEID", typeof(long));
                //returnTable.Columns.Add("TYPE", typeof(string));
                //returnTable.Columns.Add("FUNDHOUSECODE", typeof(int));
                //returnTable.Columns.Add("NAME", typeof(string));
                //returnTable.Columns.Add("SCHEMECODE", typeof(long));
                //returnTable.Columns.Add("SCHEMENAME", typeof(string));
                //returnTable.Columns.Add("NET_ASSET_VALUE", typeof(decimal));
                //returnTable.Columns.Add("NAVDATE", typeof(DateTime));


                if ((datewiseData != null) && (datewiseData.Rows.Count > 0))
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    var transaction = sqlite_conn.BeginTransaction();
                    try
                    {
                        //portfolioId = getPortfolioId(portfolioName, userId, sqlite_cmd);
                        for (DateTime dt = fromDt; dt <= System.Convert.ToDateTime(datewiseData.Rows[datewiseData.Rows.Count - 1]["NAVDATE"]); dt = getNextSIPDate(dt, Int32.Parse(monthday), sipFrequency))
                        {
                            transDt = dt;
                            do
                            {
                                datewiseData.DefaultView.RowFilter = "NAVDATE = '" + transDt.ToShortDateString() + "'";
                                if (datewiseData.DefaultView.Count > 0)
                                {
                                    break;
                                }
                                transDt = transDt.AddDays(1);
                            } while (transDt <= System.Convert.ToDateTime(datewiseData.Rows[datewiseData.Rows.Count - 1]["NAVDATE"]));
                            if (datewiseData.DefaultView.Count > 0)
                            {
                                purchaseNAV = System.Convert.ToDouble(datewiseData.DefaultView[0]["NET_ASSET_VALUE"]);
                                purchaseUnits = 0.00;
                                if (purchaseNAV > 0)
                                {
                                    purchaseUnits = Math.Round((valueAtCost / purchaseNAV), 4);
                                }
                                addNewTransaction(userId, portfolioName, schemeCode,
                                    datewiseData.DefaultView[0]["NAVDATE"].ToString(),
                                    string.Format("{0:0.0000}", purchaseNAV),
                                    string.Format("{0:0.0000}", purchaseUnits), string.Format("{0:0.0000}", valueAtCost), portfolioRowId, sqlite_cmd: sqlite_cmd);

                            }
                        }

                        //for (int rownum = 0; rownum < datewiseData.Rows.Count; rownum += getNextSIPDurationCounter(sipFrequency))
                        //{
                        //    purchaseNAV = System.Convert.ToDouble(datewiseData.Rows[rownum]["NET_ASSET_VALUE"]);

                        //    purchaseUnits = 0.00;
                        //    if (purchaseNAV > 0)
                        //    {
                        //        purchaseUnits = Math.Round((valueAtCost / purchaseNAV), 4);
                        //    }
                        //    //string.Format("{0:0.0000}", fields[4])
                        //    addNewTransaction(userId, portfolioName, schemeCode,
                        //        datewiseData.Rows[rownum]["NAVDATE"].ToString(),
                        //        string.Format("{0:0.0000}", purchaseNAV),
                        //        string.Format("{0:0.0000}", purchaseUnits), string.Format("{0:0.0000}", valueAtCost), portfolioRowId, sqlite_cmd: sqlite_cmd);
                        //}
                        breturn = true;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("addNewSIP exception: " + ex.Message);
                        breturn = false;
                    }
                    transaction.Commit();
                    transaction.Dispose();
                    transaction = null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("addNewSIP exception: " + ex.Message);
                breturn = false;
            }
            finally
            {
                if (datewiseData != null)
                {
                    datewiseData.Clear();
                    datewiseData.Dispose();
                    datewiseData = null;
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_cmd = null;
            }
            return breturn;
        }

        public bool deletePortfolioRow(string portfolioRowId)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            //long portfolioId = -1;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();

                sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE ROWID = " + portfolioRowId;

                try
                {
                    if (sqlite_cmd.ExecuteNonQuery() > 0)
                    {
                        breturn = true;
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("deletePortfolioRow: [" + portfolioRowId + "] " + exSQL.Message);
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("deletePortfolioRow exception: " + ex.Message);
                breturn = false;
            }
            finally
            {
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }
                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_cmd = null;
                sqlite_conn = null;
            }
            return breturn;
        }

        public bool updateTransaction(string userId, string portfolioName, string portfolioRowId, string oldschemeCode, string oldpurchaseDate, string oldpurchaseNAV, string oldpurchaseUnits, string oldvalueAtCost,
                                            string schemeCode, string purchaseDate, string purchaseNAV, string purchaseUnits, string valueAtCost)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            //long portfolioId = -1;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();

                //portfolioId = getPortfolioId(portfolioName, userId, sqlite_cmd);
                //if (portfolioId > 0)
                {
                    sqlite_cmd.CommandText = "UPDATE PORTFOLIO SET SCHEMECODE = @SCHEMECODE, PURCHASE_DATE = @PURCHASE_DATE, PURCHASE_NAV = @PURCHASE_NAV, " +
                                            "PURCHASE_UNITS = @PURCHASE_UNITS, VALUE_AT_COST = @VALUE_AT_COST " +
                                            "WHERE ROWID = " + portfolioRowId;


                    //sqlite_cmd.CommandText = "UPDATE PORTFOLIO SET SCHEMECODE = @SCHEMECODE, PURCHASE_DATE = @PURCHASE_DATE, PURCHASE_NAV = @PURCHASE_NAV, PURCHASE_UNITS = @PURCHASE_UNITS, VALUE_AT_COST = @VALUE_AT_COST " +
                    //                        "WHERE MASTER_ROWID = @MASTER_ROW_ID AND SCHEMECODE = @SCHEMECODE_OLD AND PURCHASE_DATE = @PURCHASE_DATE_OLD AND PURCHASE_NAV = @PURCHASE_NAV_OLD " +
                    //                        "AND PURCHASE_UNITS = @PURCHASE_UNITS_OLD AND VALUE_AT_COST = @VALUE_AT_COST_OLD";
                    sqlite_cmd.Prepare();
                    //sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", portfolioId);
                    //sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE_OLD", oldschemeCode);
                    sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(purchaseDate).ToString("yyyy-MM-dd"));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_NAV", string.Format("{0:0:0000}", purchaseNAV));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_UNITS", string.Format("{0:0.0000}", purchaseUnits));
                    sqlite_cmd.Parameters.AddWithValue("@VALUE_AT_COST", string.Format("{0:0.0000}", valueAtCost));
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE_OLD", System.Convert.ToDateTime(oldpurchaseDate).ToString("yyyy-MM-dd"));
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_NAV_OLD", string.Format("{0:0:0000}", oldpurchaseNAV));
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_UNITS_OLD", string.Format("{0:0.0000}", oldpurchaseUnits));
                    //sqlite_cmd.Parameters.AddWithValue("@VALUE_AT_COST_OLD", string.Format("{0:0.0000}", oldvalueAtCost));

                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            breturn = true;
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("updateTransaction: [" + userId + "," + portfolioName + "] " + exSQL.Message);
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("updateTransaction exception: " + ex.Message);
                breturn = false;
            }
            finally
            {
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }
                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_cmd = null;
                sqlite_conn = null;
            }
            return breturn;
        }


        public bool deletePortfolio(string userId, string portfolioMasterRowId)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            long portfolioId = -1;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();

                //portfolioId = getPortfolioId(portfolioName, userId, sqlite_cmd);
                portfolioId = Int32.Parse(portfolioMasterRowId);
                if (portfolioId > 0)
                {
                    try
                    {
                        sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE MASTER_ROWID = @MASTER_ROW_ID";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@MASTER_ROW_ID", portfolioId);
                        sqlite_cmd.ExecuteNonQuery();
                        //if (sqlite_cmd.ExecuteNonQuery() > 0)
                        //{
                        sqlite_cmd.Reset();
                        sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO_MASTER WHERE ROWID = @MASTER_ROW_ID";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@MASTER_ROW_ID", portfolioId);
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            breturn = true;
                        }
                        //}
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioMasterRowId + "] " + exSQL.Message);
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioMasterRowId + "] " + ex.Message);
                breturn = false;
            }
            finally
            {
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }
                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_cmd = null;
                sqlite_conn = null;
            }

            return breturn;
        }

        public DataTable openMFPortfolio(string userId, string portfolioFileName, string portfolioMasterRowId, bool bCurrent = true, bool bValuation = false, int historyperiod = 0)
        {
            DataTable resultDataTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            //string statement = "SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.FUNDHOUSECODE as FundHouseCode, FUNDHOUSE.NAME as FundHouse, SCHEME_TYPE.TYPE_ID, SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, " +
            //                   "strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE) AS PurchaseDate, PORTFOLIO.PURCHASE_NAV as PurchaseNAV, PORTFOLIO.PURCHASE_UNITS as PurchaseUnits, " +
            //                   "PORTFOLIO.VALUE_AT_COST as ValueAtCost, NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate from SCHEMES " +
            //                   "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
            //                   "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
            //                   "INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
            //                   "INNER JOIN SCHEME_TYPE ON SCHEME_TYPE.ROWID = SCHEMES.SCHEMETYPEID " +
            //                   "WHERE NAVRECORDS.NAVDATE = FUNDHOUSE.LAST_UPDATE_DATE AND PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
            //                   " ORDER BY SCHEMES.FUNDHOUSECODE ASC, SCHEMES.SCHEMECODE ASC, PORTFOLIO.PURCHASE_DATE ASC";
            string statement = "SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.FUNDHOUSECODE as FundHouseCode, FUNDHOUSE.NAME as FundHouse, SCHEME_TYPE.TYPE_ID, " +
                                "SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, " +
                                "strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE) AS PurchaseDate, PORTFOLIO.PURCHASE_NAV as PurchaseNAV, " +
                                "PORTFOLIO.PURCHASE_UNITS as PurchaseUnits, PORTFOLIO.VALUE_AT_COST as ValueAtCost " +
                                //"NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate " +
                                "from SCHEMES " +
                               "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
                               //"INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "INNER JOIN SCHEME_TYPE ON SCHEME_TYPE.ROWID = SCHEMES.SCHEMETYPEID " +
                               //"WHERE NAVRECORDS.NAVDATE = FUNDHOUSE.LAST_UPDATE_DATE AND PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                               "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                               " ORDER BY SCHEMES.FUNDHOUSECODE ASC, SCHEMES.SCHEMECODE ASC, PORTFOLIO.PURCHASE_DATE ASC";
            try
            {
                updateNAVForPortfolio(portfolioMasterRowId);

                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    //get all records from portfolio table matching portfolioname
                    //get related scheme name, scheme id from schemes table
                    //get latest NAV from NAVrecords table where NAVDate = schemes.todate

                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    resultDataTable = new DataTable();
                    //FundHouse;FundName;SCHEME_CODE;PurchaseDate;PurchaseNAV;PurchaseUnits;ValueAtCost
                    resultDataTable.Columns.Add("ID", typeof(long)); //FundHouse
                    resultDataTable.Columns.Add("FundHouseCode", typeof(int)); //FundHouse
                    resultDataTable.Columns.Add("FundHouse", typeof(string)); //FundHouse
                    resultDataTable.Columns.Add("TYPE_ID", typeof(string)); //scheme_type - can be 1, 2, 3, 4
                    resultDataTable.Columns.Add("FundName", typeof(string)); //FundName
                    resultDataTable.Columns.Add("SCHEME_CODE", typeof(string)); //SCHEME_CODE
                    resultDataTable.Columns.Add("PurchaseDate", typeof(DateTime)); //PurchaseDate
                    resultDataTable.Columns.Add("PurchaseNAV", typeof(decimal)); //PurchaseNAV
                    resultDataTable.Columns.Add("PurchaseUnits", typeof(decimal)); //PurchaseUnits
                    resultDataTable.Columns.Add("ValueAtCost", typeof(decimal)); //ValueAtCost

                    resultDataTable.Columns.Add("CurrentNAV", typeof(decimal));
                    resultDataTable.Columns.Add("NAVDate", typeof(DateTime));
                    resultDataTable.Columns.Add("CurrentValue", typeof(decimal));
                    resultDataTable.Columns.Add("YearsInvested", typeof(decimal));
                    resultDataTable.Columns.Add("ARR", typeof(decimal));

                    resultDataTable.RowChanged += new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
                    //resultDataTable.RowChanged += new DataRowChangeEventHandler((s, e) => handlerPortfolioTableRowChanged(s, e, sqlite_cmd));

                    ////resultDataTable.Columns.Add("CurrentValue", typeof(decimal), "Math.Round(CurrentNAV * System.Convert.ToDouble(string.Format(\"{0:0.0000}\", PurchaseUnits)), 4)");
                    //resultDataTable.Columns.Add("CurrentValue", typeof(decimal), "(CurrentNAV * System.Convert.ToDouble(string.Format(\"{0:0.0000}\", PurchaseUnits))");

                    ////resultDataTable.Columns.Add("YearsInvested", typeof(decimal), "Math.Round(((NAVDate - PurchaseDate).TotalDays) / 365.25, 4)");
                    //resultDataTable.Columns.Add("YearsInvested", typeof(decimal), "(((NAVDate - PurchaseDate).TotalDays) / 365.25)");

                    ////resultDataTable.Columns.Add("ARR", typeof(decimal), "Math.Round(Math.Pow((CurrentValue / System.Convert.ToDouble(string.Format(\"{0:0.0000}\", ValueAtCost))), (1 / YearsInvested)) - 1, 4)");
                    //resultDataTable.Columns.Add("ARR", typeof(decimal), "(Math.Pow((CurrentValue / System.Convert.ToDouble(string.Format(\"{0:0.0000}\", ValueAtCost))), (1 / YearsInvested)) - 1)");

                    resultDataTable.Load(sqlite_datareader);
                    resultDataTable.RowChanged -= new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
                    //resultDataTable.RowChanged -= new DataRowChangeEventHandler((s, e) => handlerPortfolioTableRowChanged(s, e, sqlite_cmd));
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("openMFPortfolio: " + exSQL.Message);
                    if (resultDataTable != null)
                    {
                        resultDataTable.Clear();
                        resultDataTable.Dispose();
                        resultDataTable = null;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("openMFPortfolio: " + ex.Message);
                    if (resultDataTable != null)
                    {
                        resultDataTable.Clear();
                        resultDataTable.Dispose();
                        resultDataTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                if (resultDataTable != null)
                {
                    resultDataTable.Clear();
                    resultDataTable.Dispose();
                    resultDataTable = null;
                }
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }

            return resultDataTable;
        }

        //private void handlerPortfolioTableRowChanged(object sender, DataRowChangeEventArgs e, SQLiteCommand sqlite_cmd)
        private void handlerPortfolioTableRowChanged(object sender, DataRowChangeEventArgs e)
        {
            //e.Row.Table.RowChanged -= new DataRowChangeEventHandler((s, earg) => handlerPortfolioTableRowChanged(s, e, sqlite_cmd));
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
            double currentNAV, valueAtCost, currentValue, yearsInvested, arr;
            DateTime currentNAVdt, purchaseNAVDt;
            currentNAV = 0.00; currentValue = 0.00; yearsInvested = 0.00; arr = 0.00; valueAtCost = 0.00;
            //currentNAVdt = DateTime.Today;
            currentNAVdt = DateTime.MinValue;

            //first check if this is transaction for new symbol or for same symbol. Depending that currentPrice and CurrentDate will be assigned
            DataTable sourceTable = (DataTable)sender;
            int rowIndex = sourceTable.Rows.Count;
            rowIndex--; //this is the current row represented by e
            if (sourceTable.Rows.Count > 1)
            {
                rowIndex--; //this is the row prior to e
            }

            //if this is the first row or if the current row symbol is different than previous row symbol - fetch latest NAV from NAVrecords table
            if ((rowIndex == 0) || (e.Row["SCHEME_CODE"].ToString().Equals(sourceTable.Rows[rowIndex]["SCHEME_CODE"].ToString()) == false))
            {
                SQLiteConnection sqlite_conn = CreateConnection();
                SQLiteCommand sqlite_cmd1 = sqlite_conn.CreateCommand();
                SQLiteDataReader sqlite_datareader = null;
                //sqlite_cmd1.CommandText = "SELECT NAVRECORDS.NET_ASSET_VALUE, max(strftime('%d-%m-%Y', NAVRECORDS.NAVDATE)) as NAVDATE from NAVRECORDS " +
                //                    "WHERE NAVRECORDS.SCHEMECODE = " + e.Row["SCHEME_CODE"].ToString();
                //the above statement max gives 31-12-2021 as the max instead of date 17-01-2022
                sqlite_cmd1.CommandText = "SELECT NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', max(julianday(NAVRECORDS.NAVDATE))) as NAVDATE from NAVRECORDS " +
                                    "WHERE NAVRECORDS.SCHEMECODE = " + e.Row["SCHEME_CODE"].ToString();
                try
                {
                    sqlite_datareader = sqlite_cmd1.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        currentNAV = System.Convert.ToDouble(string.Format("{0:0.00}", sqlite_datareader["NET_ASSET_VALUE"].ToString()));
                        currentNAVdt = System.Convert.ToDateTime(sqlite_datareader["NAVDATE"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("portfolio table handler error: " + exSQL.Message);
                    currentNAV = 0.00;
                    currentNAVdt = DateTime.MinValue;
                }
                finally
                {
                    if (sqlite_datareader != null)
                    {
                        sqlite_datareader.Close();
                    }
                    if (sqlite_cmd1 != null)
                    {
                        sqlite_cmd1.Dispose();
                    }
                    if (sqlite_conn != null)
                    {
                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                    }
                    sqlite_datareader = null;
                    sqlite_cmd1 = null;
                    sqlite_conn = null;
                }
            }
            else
            {
                currentNAV = System.Convert.ToDouble(string.Format("{0:0.00}", sourceTable.Rows[rowIndex]["CurrentNAV"].ToString()));
                currentNAVdt = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["NAVDate"].ToString());
            }

            if (currentNAVdt.Equals(DateTime.MinValue) == false)
            {
                //currentNAV = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["CurrentNAV"]));
                currentValue = Math.Round(currentNAV * System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["PurchaseUnits"])), 2);
                //currentNAVdt = System.Convert.ToDateTime(e.Row["NAVDate"].ToString());

                purchaseNAVDt = System.Convert.ToDateTime(e.Row["PurchaseDate"].ToString());


                try
                {
                    yearsInvested = Math.Round(((currentNAVdt - purchaseNAVDt).TotalDays) / 365.25, 2);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("openMFPortfolio: " + ex.Message);
                    yearsInvested = Math.Round(0.00, 2);
                }
                valueAtCost = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["ValueAtCost"]));

                try
                {
                    arr = Math.Round(0.00, 2);
                    if (yearsInvested > 0)
                    {
                        arr = Math.Round(Math.Pow((currentValue / valueAtCost), (1 / yearsInvested)) - 1, 2);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("openMFPortfolio: " + ex.Message);
                    arr = Math.Round(0.00, 2);
                }
            }
            else
            {
                currentValue = 0.00;
                purchaseNAVDt = DateTime.MinValue;
            }

            e.Row["PurchaseDate"] = purchaseNAVDt.ToString("yyyy-MM-dd");
            e.Row["PurchaseNAV"] = string.Format("{0:0.00}", e.Row["PurchaseNAV"]);
            e.Row["PurchaseUnits"] = string.Format("{0:0.00}", e.Row["PurchaseUnits"]);
            e.Row["ValueAtCost"] = string.Format("{0:0.00}", valueAtCost);

            e.Row["CurrentNAV"] = string.Format("{0:0.00}", currentNAV);
            e.Row["NAVDate"] = currentNAVdt.ToString("yyyy-MM-dd");
            e.Row["CurrentValue"] = string.Format("{0:0.00}", currentValue);
            e.Row["YearsInvested"] = string.Format("{0:0.00}", yearsInvested);
            e.Row["ARR"] = string.Format("{0:0.00}", arr);

            //e.Row.Table.RowChanged += new DataRowChangeEventHandler((s, earg) => handlerPortfolioTableRowChanged(s, e, sqlite_cmd));
            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
        }

        public DataTable GetMFValuationBarGraph(string portfolioMasterRowId, string userId = "", string portfolioName ="" )
        {
            DataTable resultDataTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement = "SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.NAME AS FundHouse, SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, " +
                               "min(strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE)) as FirstPurchaseDate, " +
                               "sum(PORTFOLIO.PURCHASE_UNITS) as CumulativeUnits, " +
                               "sum(PORTFOLIO.VALUE_AT_COST) as CumulativeCost " +
                               //"NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, " +
                               //"strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate, " +
                               //"(sum(PORTFOLIO.PURCHASE_UNITS) * NAVRECORDS.NET_ASSET_VALUE) as CumulativeValue " +
                               "from SCHEMES " +
                               "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
                               //"INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               //"WHERE NAVRECORDS.NAVDATE = FUNDHOUSE.LAST_UPDATE_DATE AND PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                               "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                               " GROUP BY FundName";
            try
            {

                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    resultDataTable = new DataTable();
                    //FundHouse;FundName;SCHEME_CODE;PurchaseDate;PurchaseNAV;PurchaseUnits;ValueAtCost
                    resultDataTable.Columns.Add("ID", typeof(long)); //FundHouse
                    resultDataTable.Columns.Add("FundHouse", typeof(string)); //FundHouse
                    resultDataTable.Columns.Add("FundName", typeof(string)); //FundName
                    resultDataTable.Columns.Add("SCHEME_CODE", typeof(string)); //SCHEME_CODE
                    resultDataTable.Columns.Add("FirstPurchaseDate", typeof(DateTime)); //Min of purchase date
                    resultDataTable.Columns.Add("CumulativeUnits", typeof(decimal)); //PurchaseUnits
                    resultDataTable.Columns.Add("CumulativeCost", typeof(decimal)); //ValueAtCost
                    resultDataTable.Columns.Add("CurrentNAV", typeof(decimal));
                    resultDataTable.Columns.Add("NAVDate", typeof(DateTime));
                    resultDataTable.Columns.Add("CumulativeValue", typeof(decimal));
                    resultDataTable.Columns.Add("TotalYearsInvested", typeof(decimal));
                    resultDataTable.Columns.Add("TotalARR", typeof(decimal));
                    resultDataTable.RowChanged += new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);
                    resultDataTable.Load(sqlite_datareader);
                    resultDataTable.RowChanged -= new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("GetMFValuationBarGraph: " + exSQL.Message);
                    if (resultDataTable != null)
                    {
                        resultDataTable.Clear();
                        resultDataTable.Dispose();
                        resultDataTable = null;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("GetMFValuationBarGraph: " + ex.Message);
                    if (resultDataTable != null)
                    {
                        resultDataTable.Clear();
                        resultDataTable.Dispose();
                        resultDataTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetMFValuationBarGraph: " + ex.Message);
                if (resultDataTable != null)
                {
                    resultDataTable.Clear();
                    resultDataTable.Dispose();
                    resultDataTable = null;
                }
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }

            return resultDataTable;
        }

        private void handlerMFValuationBarGraphRowChanged(object sender, DataRowChangeEventArgs e)
        {
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);

            double currentNAV, valueAtCost, currentValue, yearsInvested, arr, cumunits = 0.00;
            //"(sum(PORTFOLIO.PURCHASE_UNITS) * NAVRECORDS.NET_ASSET_VALUE) as CumulativeValue " +

            DateTime currentNAVdt, firstPurchaseNAVDt;
            
            currentNAV = 0.00; currentValue = 0.00; yearsInvested = 0.00; arr = 0.00; valueAtCost = 0.00;



            //currentNAVdt = DateTime.Today;
            currentNAVdt = DateTime.MinValue;
            //string fundName = e.Row["FundName"].ToString().Replace("'", "\'");

            //first check if this is transaction for new symbol or for same symbol. Depending that currentPrice and CurrentDate will be assigned
            DataTable sourceTable = (DataTable)sender;
            int rowIndex = sourceTable.Rows.Count;
            rowIndex--; //this is the current row represented by e
            if (sourceTable.Rows.Count > 1)
            {
                rowIndex--; //this is the row prior to e
            }

            //if this is the first row or if the current row symbol is different than previous row symbol - fetch latest NAV from NAVrecords table
            if ((rowIndex == 0) || (e.Row["SCHEME_CODE"].ToString().Equals(sourceTable.Rows[rowIndex]["SCHEME_CODE"].ToString()) == false))
            {
                SQLiteConnection sqlite_conn = CreateConnection();
                SQLiteCommand sqlite_cmd1 = sqlite_conn.CreateCommand();
                SQLiteDataReader sqlite_datareader = null;
                //sqlite_cmd1.CommandText = "SELECT NAVRECORDS.NET_ASSET_VALUE, max(strftime('%d-%m-%Y', NAVRECORDS.NAVDATE)) as NAVDATE from NAVRECORDS " +
                //                    "WHERE NAVRECORDS.SCHEMECODE = " + e.Row["SCHEME_CODE"].ToString();
                sqlite_cmd1.CommandText = "SELECT NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', max(julianday(NAVRECORDS.NAVDATE))) as NAVDATE from NAVRECORDS " +
                                    "WHERE NAVRECORDS.SCHEMECODE = " + e.Row["SCHEME_CODE"].ToString();
                //strftime('%d-%m-%Y', max(julianday(NAVRECORDS.NAVDATE)))
                try
                {
                    sqlite_datareader = sqlite_cmd1.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        currentNAV = System.Convert.ToDouble(string.Format("{0:0.00}", sqlite_datareader["NET_ASSET_VALUE"].ToString()));
                        currentNAVdt = System.Convert.ToDateTime(sqlite_datareader["NAVDATE"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("portfolio table handler error: " + exSQL.Message);
                    currentNAV = 0.00;
                    currentNAVdt = DateTime.MinValue;
                }
                finally
                {
                    if (sqlite_datareader != null)
                    {
                        sqlite_datareader.Close();
                    }
                    if (sqlite_cmd1 != null)
                    {
                        sqlite_cmd1.Dispose();
                    }
                    if (sqlite_conn != null)
                    {
                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                    }
                    sqlite_datareader = null;
                    sqlite_cmd1 = null;
                    sqlite_conn = null;
                }
            }
            else
            {
                currentNAV = System.Convert.ToDouble(string.Format("{0:0.00}", sourceTable.Rows[rowIndex]["CurrentNAV"].ToString()));
                currentNAVdt = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["NAVDate"].ToString());
            }

            if (currentNAVdt.Equals(DateTime.MinValue) == false)
            {
                firstPurchaseNAVDt = System.Convert.ToDateTime(e.Row["FirstPurchaseDate"].ToString());
                //"(sum(PORTFOLIO.PURCHASE_UNITS) * NAVRECORDS.NET_ASSET_VALUE) as CumulativeValue " +
                cumunits = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["CumulativeUnits"]));
                currentValue = cumunits * currentNAV;
                //currentValue = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["CumulativeValue"]));
                
                valueAtCost = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["CumulativeCost"]));

                try
                {
                    yearsInvested = Math.Round(((currentNAVdt - firstPurchaseNAVDt).TotalDays) / 365.25, 4);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("openMFPortfolio: " + ex.Message);
                    yearsInvested = Math.Round(0.00, 2);
                }

                try
                {
                    arr = Math.Round(0.00, 2);
                    if (yearsInvested > 0)
                    {
                        arr = Math.Round(Math.Pow((currentValue / valueAtCost), (1 / yearsInvested)) - 1, 2);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("openMFPortfolio: " + ex.Message);
                    arr = Math.Round(0.00, 2);
                }
            }
            //e.Row["FundName"] = fundName;
            e.Row["CurrentNAV"] = string.Format("{0:0.00}", currentNAV);
            e.Row["NAVDate"] = currentNAVdt.ToShortDateString(); // .ToString("yyyy-MM-dd");

            e.Row["CumulativeUnits"] = string.Format("{0:0.00}", cumunits);
            e.Row["CumulativeCost"] = string.Format("{0:0.00}", valueAtCost);

            e.Row["CumulativeValue"] = string.Format("{0:0.00}", currentValue);

            e.Row["TotalYearsInvested"] = string.Format("{0:0.00}", yearsInvested);
            e.Row["TotalARR"] = string.Format("{0:0.00}", arr);

            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);
        }

        public DataTable GetValuationLineGraph(string portfolioMasterRowId, string userId = "", string portfolioName = "")
        {
            DataTable valuationTable = new DataTable();
            DataTable portfolioSummaryTable;
            DataTable portfolioTransactionTable;
            DataTable navTable;

            DateTime currentTxnDate;
            DateTime nextTxnDate;
            DateTime navDate;

            double cumulativeQty = 0;
            double cumulativeCost = 0;
            double currentNAV = 0;
            double currentVal = 0;

            int navRowNum = 0;

            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;

            string statement;

            try
            {
                //First get the schemes & their first & last purchase date & cumulative units
                //statement = "SELECT SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, min(strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE)) as FirstPurchaseDate, " +
                statement = "SELECT SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, min(PORTFOLIO.PURCHASE_DATE) as FirstPurchaseDate, " +
                    //"max(strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE)) as LastPurchaseDate, sum(PORTFOLIO.PURCHASE_UNITS) as SUMUNITS FROM PORTFOLIO " +
                    "max(PORTFOLIO.PURCHASE_DATE) as LastPurchaseDate, sum(PORTFOLIO.PURCHASE_UNITS) as SUMUNITS FROM PORTFOLIO " +
                    "INNER JOIN SCHEMES ON SCHEMES.SCHEMECODE = PORTFOLIO.SCHEMECODE " +
                    "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId +
                    " GROUP BY PORTFOLIO.SCHEMECODE " +
                    "ORDER BY PORTFOLIO.SCHEMECODE ASC, FirstPurchaseDate ASC";

                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                try
                {
                    sqlite_cmd.CommandText = statement;
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    portfolioSummaryTable = new DataTable();
                    portfolioSummaryTable.Load(sqlite_datareader);
                    sqlite_datareader.Close();

                    //NOw we have name of each scheme & first purchase date
                    //Now get transaction records for specific scheme
                    foreach (DataRow summaryRow in portfolioSummaryTable.Rows)
                    {
                        //first get the NAV records for the current schemecode & navdate is >= summary rows First Purchase Date
                        statement = "SELECT SCHEMES.SCHEMECODE AS SCHEME_CODE, SCHEMES.SCHEMENAME AS SCHEME_NAME, NAVRECORDS.NET_ASSET_VALUE as NET_ASSET_VALUE, " +
                                    "strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as DATE from NAVRECORDS " +
                                    "INNER JOIN SCHEMES ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
                                    "WHERE NAVRECORDS.SCHEMECODE = " + summaryRow["SCHEMECODE"] +
                                    " AND NAVRECORDS.NAVDATE >= '" + summaryRow["FirstPurchaseDate"] + "'" +
                                    " ORDER BY NAVRECORDS.NAVDATE ASC";
                        sqlite_cmd.CommandText = statement;
                        sqlite_datareader = sqlite_cmd.ExecuteReader();
                        navTable = new DataTable();
                        navTable.Columns.Add("SCHEME_CODE", typeof(string));
                        navTable.Columns.Add("SCHEME_NAME", typeof(string));
                        //Following columns are from NAVRECORDS
                        navTable.Columns.Add("NET_ASSET_VALUE", typeof(decimal));
                        navTable.Columns.Add("DATE", typeof(DateTime));
                        //following comes from portfolio table
                        navTable.Columns.Add("PurchaseDate", typeof(DateTime)); //PurchaseDate
                        navTable.Columns.Add("PurchaseNAV", typeof(decimal)); //PurchaseNAV
                        navTable.Columns.Add("PurchaseUnits", typeof(decimal)); //PurchaseUnits
                        navTable.Columns.Add("ValueAtCost", typeof(decimal)); //ValueAtCost
                        //following are calculated
                        navTable.Columns.Add("CumulativeUnits", typeof(decimal)); //CumulativeUnits
                        navTable.Columns.Add("CumulativeCost", typeof(decimal)); //CumulativeCost
                        navTable.Columns.Add("CurrentValue", typeof(decimal));
                        navTable.Load(sqlite_datareader);
                        sqlite_datareader.Close();
                        //Now we have records from NAVRecords table for the current scheme & the NAV date >= first puchase date

                        //now get the transaction records for the specific schemecode from portfolio table
                        statement = "SELECT SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE) as PURCHASE_DATE, PORTFOLIO.PURCHASE_NAV, " +
                                    "PORTFOLIO.PURCHASE_UNITS, PORTFOLIO.VALUE_AT_COST FROM SCHEMES " +
                                    "INNER JOIN PORTFOLIO ON SCHEMES.SCHEMECODE = PORTFOLIO.SCHEMECODE " +
                                    "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioMasterRowId + " AND PORTFOLIO.SCHEMECODE = " + summaryRow["SCHEMECODE"] +
                                    " ORDER BY PORTFOLIO.SCHEMECODE ASC, PORTFOLIO.PURCHASE_DATE ASC";
                        sqlite_cmd.CommandText = statement;
                        sqlite_datareader = sqlite_cmd.ExecuteReader();
                        portfolioTransactionTable = new DataTable();
                        portfolioTransactionTable.Load(sqlite_datareader);
                        sqlite_datareader.Close();

                        navRowNum = 0;
                        cumulativeQty = 0;
                        cumulativeCost = 0;
                        currentVal = 0;
                        for (int txnRowNum = 0; txnRowNum < portfolioTransactionTable.Rows.Count; txnRowNum++)
                        {
                            currentTxnDate = System.Convert.ToDateTime(portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_DATE"].ToString());
                            if (txnRowNum + 1 >= portfolioTransactionTable.Rows.Count)
                            {
                                nextTxnDate = DateTime.Today;
                            }
                            else
                            {
                                nextTxnDate = System.Convert.ToDateTime(portfolioTransactionTable.Rows[txnRowNum + 1]["PURCHASE_DATE"].ToString());
                            }

                            cumulativeQty += System.Convert.ToDouble(portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_UNITS"]);
                            cumulativeCost += System.Convert.ToDouble(portfolioTransactionTable.Rows[txnRowNum]["VALUE_AT_COST"]);

                            //navDate = System.Convert.ToDateTime(navTable.Rows[navRowNum]["DATE"].ToString());
                            while ((navRowNum < navTable.Rows.Count) && ((navDate = System.Convert.ToDateTime(navTable.Rows[navRowNum]["DATE"].ToString())) < nextTxnDate))
                            {

                                currentNAV = System.Convert.ToDouble(navTable.Rows[navRowNum]["NET_ASSET_VALUE"]);
                                currentVal = cumulativeQty * currentNAV;

                                navTable.Rows[navRowNum]["PurchaseDate"] = currentTxnDate;
                                navTable.Rows[navRowNum]["PurchaseNAV"] = System.Convert.ToDouble(portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_NAV"]);
                                navTable.Rows[navRowNum]["PurchaseUnits"] = System.Convert.ToDouble(portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_UNITS"]);
                                navTable.Rows[navRowNum]["ValueAtCost"] = System.Convert.ToDouble(portfolioTransactionTable.Rows[txnRowNum]["VALUE_AT_COST"]);

                                //Now fill the cumulative values
                                navTable.Rows[navRowNum]["CumulativeUnits"] = cumulativeQty;
                                navTable.Rows[navRowNum]["CumulativeCost"] = cumulativeCost;
                                navTable.Rows[navRowNum]["CurrentValue"] = currentVal;

                                navRowNum++;
                            }
                            //now we have added portfolio data to nav records table
                        }
                        //now we have complete portfolio data including cumulative values in the NAV records table
                        //now merge the nav records table in valuation table

                        valuationTable.Merge(navTable, true);

                        //now clear the data tables;
                        navTable.Rows.Clear();
                        navTable.Clear();
                        navTable.Dispose();
                        navTable = null;

                        portfolioTransactionTable.Rows.Clear();
                        portfolioTransactionTable.Clear();
                        portfolioTransactionTable.Dispose();
                        portfolioTransactionTable = null;
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("GetMFValuationLineGraph: " + exSQL.Message);
                    if (valuationTable != null)
                    {
                        valuationTable.Clear();
                        valuationTable.Dispose();
                        valuationTable = null;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("GetMFValuationLineGraph: " + ex.Message);
                    if (valuationTable != null)
                    {
                        valuationTable.Clear();
                        valuationTable.Dispose();
                        valuationTable = null;
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetMFValuationLineGraph: " + ex.Message);
                if (valuationTable != null)
                {
                    valuationTable.Clear();
                    valuationTable.Dispose();
                    valuationTable = null;
                }
            }
            finally
            {
                if (sqlite_datareader != null)
                {
                    sqlite_datareader.Close();
                }
                if (sqlite_cmd != null)
                {
                    sqlite_cmd.Dispose();
                }

                if (sqlite_conn != null)
                {
                    sqlite_conn.Close();
                    sqlite_conn.Dispose();
                }
                sqlite_conn = null;
                sqlite_datareader = null;
                sqlite_cmd = null;
            }
            return valuationTable;
        }

        #endregion
        #region Graph methods
        /// <summary>
        ///we can take the previous sum, subtract the oldest value, and add the new value. That gives us the new sum, which we can divide by 3 to get the SMA. 
        /// </summary>
        /// <param name="schemecode"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="smallPeriod"></param>
        /// <param name="longPeriod"></param>
        /// <returns>daily NAV table with SMA values and cross over flag = true if small sma > long sma else false</returns>
        public DataTable getSMATable(int schemecode, string fromDate = null, string toDate = null, int smallPeriod = 10, int longPeriod = -1)
        {
            DataTable dailyTable = null;
            try
            {
                dailyTable = getNAVRecordsTable(schemecode, fromDate, toDate);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    DataColumn newCol = new DataColumn("SMA_SMALL", typeof(decimal));
                    newCol.DefaultValue = 0;
                    dailyTable.Columns.Add(newCol);

                    if (longPeriod > 0)
                    {
                        newCol = new DataColumn("SMA_LONG", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("CROSSOVER_FLAG", typeof(string));
                        newCol.DefaultValue = "LT";
                        dailyTable.Columns.Add(newCol);
                    }
                    double currentNavValue = 0;
                    double smallSMA = 0;
                    double longSMA = 0;

                    double sumSmall = 0;
                    double[] valuesSmall = new double[smallPeriod]; //array of NAV for the current iteration
                    int indexSmall = 0; //we will increment it till specifid period and then reset it to 0

                    double sumLong = 0;
                    double[] valuesLong = (longPeriod > 0) ? new double[longPeriod] : null;
                    int indexLong = 0;

                    for (int i = 0; i < dailyTable.Rows.Count; i++)
                    {
                        currentNavValue = System.Convert.ToDouble(dailyTable.Rows[i]["NET_ASSET_VALUE"]);
                        //subtract the oldest NAV from the previous SUM and then add the current NAV
                        sumSmall = sumSmall - valuesSmall[indexSmall] + currentNavValue;//System.Convert.ToDouble(dailyTable.Rows[i]["NET_ASSET_VALUE"]);
                        valuesSmall[indexSmall] = currentNavValue; //System.Convert.ToDouble(dailyTable.Rows[i]["NET_ASSET_VALUE"]);

                        dailyTable.Rows[i]["SMA_SMALL"] = smallSMA = sumSmall / smallPeriod;
                        indexSmall = (indexSmall + 1) % smallPeriod;
                        if (longPeriod > 0)
                        {
                            sumLong = sumLong - valuesLong[indexLong] + currentNavValue;  //System.Convert.ToDouble(dailyTable.Rows[i]["NET_ASSET_VALUE"]);
                            valuesLong[indexLong] = currentNavValue; //System.Convert.ToDouble(dailyTable.Rows[i]["NET_ASSET_VALUE"]);
                            dailyTable.Rows[i]["SMA_LONG"] = longSMA = sumLong / longPeriod;
                            indexLong = (indexLong + 1) % longPeriod;

                            dailyTable.Rows[i]["CROSSOVER_FLAG"] = (smallSMA > longSMA) ? "GT" : "LT";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSMAFromDailyNAV exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }
        /// <summary>
        /// From the golden cross-over point, we look ahead buyspan number of rows. If all the rows from current row till buyspan have smallsma > longsma 
        /// and if the row after buyspan is smallsma < longsma, then the current row is marked for BUY
        /// If the current row is marked for BUY then row after sellspan is marked for SELL
        /// 
        /// For each row in DAILY NAV table
        ///     if cross-over = true (ie smallSMA > longSMA, in case smallSMA < longSMA then the value is false)
        ///         check if ALL of the next 'buySpan' number of rows cross-over is true and if yes
        ///             check if the value of corss-over for current row + buyspanrow + 1 is false
        ///                 if it is then 
        ///                     we can mark current rows BUY-FLAG = true
        ///                     we can mark the row at current row + sellspan SELL-FLAG = true
        ///                
        ///  General logic for backtest strategy for buy & sell (assumming buy span = 2 & sell span = 20):
        ///     First find the small & long sma for each day
        ///     If small sma is > long sma then we mark that row as true else false
        ///     for each row that is marked as true
        ///         find if next two row's are also marked as true, if yes
        ///             then find if the 3rd row is false (meaning small sma < long sma), if yes then
        ///                 mark the 3rd row with false value as 'BUY'
        ///                 mark the 20th row from current row as 'SELL'
        ///             
        /// </summary>
        /// <param name="schemecode"></param>
        /// <param name="fromDate"></param>
        /// <param name="toDate"></param>
        /// <param name="smallPeriod"></param>
        /// <param name="longPeriod"></param>
        /// <param name="buySpan">number of rows to check from current row where each rows smallSMA is greater than longSMA</param>
        /// <param name="sellSpan">number of rows after current row where we can mark SELL if all conditions are satisfied</param>
        /// <returns></returns>
        public DataTable getBacktestFromSMA(int schemecode, string fromDate = null, string toDate = null, int smallPeriod = 10, int longPeriod = 20, int buySpan = 2, int sellSpan = 20,
                                            double simulationQty = 100)
        {
            DataTable dailyTable = null;
            double buyNAV = 0.00, sellNAV = 0.00;
            double buyCost = 0.00;
            double sellValue = 0.00;
            double profit_loss = 0.00;
            StringBuilder resultString = new StringBuilder();
            try
            {
                dailyTable = getSMATable(schemecode, fromDate, toDate, smallPeriod, longPeriod);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    DataColumn newCol = new DataColumn("BUY_FLAG", typeof(bool));
                    newCol.DefaultValue = false;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("SELL_FLAG", typeof(bool));
                    newCol.DefaultValue = false;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("QUANTITY", typeof(double));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("BUY_COST", typeof(double));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("SELL_VALUE", typeof(double));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("PROFIT_LOSS", typeof(double));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("RESULT", typeof(string));
                    newCol.DefaultValue = "";
                    dailyTable.Columns.Add(newCol);

                    bool bBuyFlag;
                    //get small sma
                    for (int i = 0; i < dailyTable.Rows.Count; i++)
                    {
                        //if smallSMA > longSMA
                        if (((dailyTable.Rows[i]["CROSSOVER_FLAG"]).ToString().Equals("GT") == true) && ((i + buySpan) < dailyTable.Rows.Count))
                        {
                            bBuyFlag = true;
                            //first check if all the next buySpan rows[CROSSOVER_FLAF] = true
                            for (int crossindex = i + 1; crossindex <= (i + buySpan); crossindex++)
                            {
                                if ((dailyTable.Rows[crossindex]["CROSSOVER_FLAG"]).ToString().Equals("GT") == false)
                                {
                                    bBuyFlag = false;
                                    break;
                                }
                            }
                            //if buyflag is true that means we have all the current row to buyspan row's crossover_flag = true
                            //now check if crossover_flag for next index after buyspan index is false and if it is then it is our 'BUY' flag = true state
                            //first check if we are not going over the table rows
                            if (((i + buySpan + 1) < dailyTable.Rows.Count) && (bBuyFlag == true))
                            {
                                if ((dailyTable.Rows[i + buySpan + 1]["CROSSOVER_FLAG"]).ToString().Equals("GT") == false)
                                {
                                    buyNAV = System.Convert.ToDouble(dailyTable.Rows[i + buySpan + 1]["NET_ASSET_VALUE"]);
                                    buyCost = simulationQty * buyNAV;

                                    //set start point
                                    dailyTable.Rows[i]["CROSSOVER_FLAG"] = "X";

                                    //set crossover point for buy
                                    dailyTable.Rows[i + buySpan + 1]["BUY_FLAG"] = true;
                                    dailyTable.Rows[i + buySpan + 1]["QUANTITY"] = simulationQty;
                                    dailyTable.Rows[i + buySpan + 1]["BUY_COST"] = buyCost;

                                    if ((i + sellSpan) < dailyTable.Rows.Count)
                                    {
                                        //set sell point
                                        sellNAV = System.Convert.ToDouble(dailyTable.Rows[i + sellSpan]["NET_ASSET_VALUE"]);
                                        sellValue = simulationQty * sellNAV;
                                        profit_loss = sellValue - buyCost;

                                        dailyTable.Rows[i + sellSpan]["QUANTITY"] = simulationQty;
                                        dailyTable.Rows[i + sellSpan]["SELL_FLAG"] = true;
                                        dailyTable.Rows[i + sellSpan]["SELL_VALUE"] = sellValue;
                                        dailyTable.Rows[i + sellSpan]["PROFIT_LOSS"] = profit_loss;

                                        resultString.AppendLine("BUY Date: " + dailyTable.Rows[i + buySpan + 1]["NAVDATE"]);
                                        resultString.AppendLine("BUY NAV: " + buyNAV);
                                        resultString.AppendLine("BUY Cost: " + buyCost);
                                        resultString.AppendLine("SELL Date: " + dailyTable.Rows[i + sellSpan]["NAVDATE"]);
                                        resultString.AppendLine("SELL NAV: " + sellNAV);
                                        resultString.AppendLine("SELL Value: " + sellValue);
                                        resultString.AppendLine((profit_loss < 0 ? "Loss of: " : "Profit of: ") + profit_loss);
                                        dailyTable.Rows[i + sellSpan]["RESULT"] = resultString.ToString();
                                        resultString.Clear();
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSMAFromDailyNAV exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }
        //public DataTable getRSIDataTableFromDailyNAV(int schemecode, string fromDate = null, string toDate = null, string period = "14")
        //{
        //    DataTable dailyTable = null;
        //    //DataTable rsiDataTable = null;
        //    int iPeriod;
        //    double change, gain, loss, avgGain = 0.00, avgLoss = 0.00, rs, rsi;
        //    double sumOfGain = 0.00, sumOfLoss = 0.00;
        //    DateTime dateCurrentRow = DateTime.Today;

        //    try
        //    {
        //        dailyTable = getNAVRecordsTable(schemecode, fromDate, toDate);
        //        if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
        //        {
        //            iPeriod = System.Convert.ToInt32(period);
        //            //rsiDataTable = new DataTable();

        //            //rsiDataTable.Columns.Add("SCHEMECODE", typeof(long));
        //            //rsiDataTable.Columns.Add("SCHEMENAME", typeof(string));
        //            //rsiDataTable.Columns.Add("Date", typeof(DateTime));
        //            //rsiDataTable.Columns.Add("RSI", typeof(decimal));
        //            DataColumn newCol = new DataColumn("RSI", typeof(decimal));
        //            newCol.DefaultValue = 0.00;

        //            dailyTable.Columns.Add(newCol);

        //            //Strat from 1st row in dailyTable and sum all the "seriestype" column upto "period"
        //            //SMA = divide the sum by "period"
        //            //Store the symbol, Date from the last row of the current set and SMA in the smaDataTable

        //            for (int rownum = 1; rownum < dailyTable.Rows.Count; rownum++)
        //            {
        //                //current - prev
        //                change = System.Convert.ToDouble(dailyTable.Rows[rownum]["NET_ASSET_VALUE"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["NET_ASSET_VALUE"]);
        //                dateCurrentRow = System.Convert.ToDateTime(dailyTable.Rows[rownum]["NAVDATE"]);

        //                if (change < 0)
        //                {
        //                    loss = change;
        //                    gain = 0.00;
        //                }
        //                else
        //                {
        //                    gain = change;
        //                    loss = 0.00;
        //                }

        //                //for the first iPeriod keep adding loss & gain
        //                if (rownum < iPeriod)
        //                {
        //                    sumOfGain += gain;
        //                    sumOfLoss += loss;
        //                }
        //                else
        //                {
        //                    if (rownum == iPeriod)
        //                    {
        //                        sumOfGain += gain;
        //                        sumOfLoss += loss;
        //                        //we also find  other fields and SAVE
        //                        avgGain = sumOfGain / iPeriod;
        //                        avgLoss = sumOfLoss / iPeriod;
        //                        rs = avgGain / avgLoss;
        //                        rsi = 100 - (100 / (1 - rs));
        //                        //rsiDataTable.Rows.Add(new object[] {
        //                        //                                    schemecode,
        //                        //                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
        //                        //                                    dateCurrentRow.ToString("dd-MM-yyyy"),
        //                        //                                    Math.Round(rsi, 4)
        //                        //                                });
        //                    }
        //                    else
        //                    {
        //                        avgGain = ((avgGain * (iPeriod - 1)) + gain) / iPeriod;
        //                        avgLoss = ((avgLoss * (iPeriod - 1)) + loss) / iPeriod;
        //                        rs = avgGain / avgLoss;
        //                        rsi = 100 - (100 / (1 - rs));
        //                        //rsiDataTable.Rows.Add(new object[] {
        //                        //                                    schemecode,
        //                        //                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
        //                        //                                    dateCurrentRow.ToString("dd-MM-yyyy"),
        //                        //                                    Math.Round(rsi, 4)
        //                        //                                });
        //                    }
        //                    dailyTable.Rows[rownum]["RSI"] = Math.Round(rsi, 4);
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine("getRSIDataTableFromDailyNAV exception: " + ex.Message);

        //        if (dailyTable != null)
        //        {
        //            dailyTable.Clear();
        //            dailyTable.Dispose();
        //        }
        //        dailyTable = null;
        //    }
        //    return dailyTable;
        //}

        public DataTable getRSIDataTableFromDailyNAV(int schemecode, string seriestype = "NET_ASSES_VALUE",
                                    string fromDate = null, string toDate = null, string period = "14", DataTable sourceTable = null)
        {
            DataTable dailyTable = null;
            //DataTable rsiDataTable = null;
            int iPeriod;
            double change, gain, loss, avgGain = 0.00, avgLoss = 0.00, rs, rsi;
            double sumOfGain = 0.00, sumOfLoss = 0.00;
            //DateTime dateCurrentRow = DateTime.Today;
            try
            {
                if (sourceTable == null)
                {
                    dailyTable = getNAVRecordsTable(schemecode, fromDate, toDate);
                }
                else
                {
                    dailyTable = new DataTable();
                    dailyTable.Merge(sourceTable, true);
                    if (dailyTable.Columns.Contains("RSI"))
                    {
                        dailyTable.Columns.Remove("RSI");
                    }
                }
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    iPeriod = System.Convert.ToInt32(period);
                    DataColumn newCol;
                    newCol = new DataColumn("RSI"); //+ seriestype, typeof(decimal));
                    newCol.DefaultValue = 0.00;

                    dailyTable.Columns.Add(newCol);
                    change = gain = loss = avgGain = avgLoss = rs = rsi = 0.00;
                    sumOfGain = sumOfLoss = 0.00;

                    for (int rownum = 1; rownum < dailyTable.Rows.Count; rownum++)
                    {
                        //current - prev
                        //change = System.Convert.ToDouble(dailyTable.Rows[rownum][seriestype]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1][seriestype]);
                        change = System.Convert.ToDouble(dailyTable.Rows[rownum][seriestype]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1][seriestype]);
                        //dateCurrentRow = System.Convert.ToDateTime(dailyTable.Rows[rownum]["TIMESTAMP"]);

                        if (change < 0)
                        {
                            loss = Math.Abs(change);
                            gain = 0.00;
                        }
                        else
                        {
                            gain = change;
                            loss = 0.00;
                        }

                        //for the first iPeriod keep adding loss & gain
                        if (rownum < iPeriod)
                        {
                            sumOfGain += gain;
                            sumOfLoss += loss;
                        }
                        else
                        {
                            if (rownum == iPeriod)
                            {
                                sumOfGain += gain;
                                sumOfLoss += loss;
                                //we also find  other fields and SAVE
                                avgGain = sumOfGain / iPeriod;
                                avgLoss = sumOfLoss / iPeriod;
                                rs = avgGain / avgLoss;
                                rsi = 100 - (100 / (1 + rs));
                                //rsiDataTable.Rows.Add(new object[] {
                                //                                    schemecode,
                                //                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
                                //                                    dateCurrentRow.ToString("dd-MM-yyyy"),
                                //                                    Math.Round(rsi, 4)
                                //                                });
                            }
                            else
                            {
                                avgGain = ((avgGain * (iPeriod - 1)) + gain) / iPeriod;
                                avgLoss = ((avgLoss * (iPeriod - 1)) + loss) / iPeriod;
                                rs = avgGain / avgLoss;
                                rsi = 100 - (100 / (1 + rs));
                                //rsiDataTable.Rows.Add(new object[] {
                                //                                    schemecode,
                                //                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
                                //                                    dateCurrentRow.ToString("dd-MM-yyyy"),
                                //                                    Math.Round(rsi, 4)
                                //                                });
                            }
                            dailyTable.Rows[rownum]["RSI"] = Math.Round(rsi, 2);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getRSIDataTableFromDaily exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }

        /// <summary>
        /// Method to calculate SMA from daily close price for the period starting from from date specified
        /// First daily price is fetched and then depending on parameters SMA small & SMA long are added to the same table
        /// If both SMA are to be found then it also marks a row from where cross over can be identified
        /// If small SMA is grater than long SMA it will mark the current row as GT else it will mark it as LT.
        /// This value can be used to identifie golden cross over
        /// SMA is calculated using following logic to loop the daily table
        /// we can take the previous sum, subtract the oldest value, and add the new value. That gives us the new sum, which we can divide by 3 to get the SMA. 
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="exchangename"></param>
        /// <param name="type"></param>
        /// <param name="outputsize"></param>
        /// <param name="time_interval"></param>
        /// <param name="fromDate"></param>
        /// <param name="smallPeriod"></param>
        /// <param name="long_slow_Period"></param>
        /// <param name="sqlite_cmd"></param>
        /// <returns></returns>
        public DataTable GetSMA_EMA_MACD_BBANDS_Table(int schemecode, string seriestype = "NET_ASSET_VALUE",
                                    string fromDate = null, string toDate = null, int small_fast_Period = 10, int long_slow_Period = -1, bool emaRequired = false,
                                    bool macdRequired = false,
                                    //string fastperiod = "12", string slowperiod = "26", 
                                    int signalperiod = 9, bool bbandsRequired = false, int stddeviation = 2, DataTable sourceTable = null)
        {
            DataTable dailyTable = null;
            try
            {
                //DAILYID, SYMBOL, EXCHANGE, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, TIMESTAMP
                if (sourceTable == null)
                {
                    dailyTable = getNAVRecordsTable(schemecode, fromDate, toDate);
                }
                else
                {
                    dailyTable = new DataTable();
                    dailyTable.Merge(sourceTable, true);
                }

                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    if (dailyTable.Columns.Contains("SMA_SMALL"))
                    {
                        dailyTable.Columns.Remove("SMA_SMALL");
                    }
                    if (dailyTable.Columns.Contains("SMA_LONG"))
                    {
                        dailyTable.Columns.Remove("SMA_LONG");
                    }
                    if (dailyTable.Columns.Contains("CROSSOVER_FLAG"))
                    {
                        dailyTable.Columns.Remove("CROSSOVER_FLAG");
                    }
                    DataColumn newCol = new DataColumn("SMA_SMALL", typeof(decimal));
                    newCol.DefaultValue = 0;
                    dailyTable.Columns.Add(newCol);

                    //if (longPeriod > 0)
                    //{
                    newCol = new DataColumn("SMA_LONG", typeof(decimal));
                    newCol.DefaultValue = 0;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("CROSSOVER_FLAG", typeof(string));
                    newCol.DefaultValue = "LT";
                    dailyTable.Columns.Add(newCol);
                    //}
                    double currentClosePrice = 0;
                    double smallSMA = 0;
                    double longSMA = 0;

                    double sumSmall = 0;
                    double[] valuesSmall = (small_fast_Period > 0) ? new double[small_fast_Period] : null; //array of CLOSE PRICE for the current iteration
                    int indexSmall = 0; //we will increment it till specifid period and then reset it to 0

                    double sumLong = 0;
                    double[] valuesLong = (long_slow_Period > 0) ? new double[long_slow_Period] : null;
                    int indexLong = 0;

                    for (int i = 0; i < dailyTable.Rows.Count; i++)
                    {
                        //currentClosePrice = System.Convert.ToDouble(string.Format("{0:0.00}",
                        //    seriestype.Equals("CLOSE") ? dailyTable.Rows[i]["CLOSE"] : seriestype.Equals("OPEN") ? dailyTable.Rows[i]["OPEN"] :
                        //    seriestype.Equals("HIGH") ? dailyTable.Rows[i]["HIGH"] : dailyTable.Rows[i]["LOW"]));
                        currentClosePrice = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[i][seriestype].ToString()));

                        if (small_fast_Period > 0)
                        {//subtract the oldest CLOSE PRICE from the previous SUM and then add the current CLOSE PRICE
                            sumSmall = sumSmall - valuesSmall[indexSmall] + currentClosePrice;
                            valuesSmall[indexSmall] = currentClosePrice;

                            dailyTable.Rows[i]["SMA_SMALL"] = smallSMA = Math.Round((sumSmall / small_fast_Period), 2);
                            indexSmall = (indexSmall + 1) % small_fast_Period;
                        }
                        if (long_slow_Period > 0)
                        {
                            sumLong = sumLong - valuesLong[indexLong] + currentClosePrice;
                            valuesLong[indexLong] = currentClosePrice;
                            dailyTable.Rows[i]["SMA_LONG"] = longSMA = Math.Round((sumLong / long_slow_Period), 2);
                            indexLong = (indexLong + 1) % long_slow_Period;

                            dailyTable.Rows[i]["CROSSOVER_FLAG"] = (smallSMA > longSMA) ? "GT" : "LT";
                        }
                    }

                    if ((bbandsRequired) && (small_fast_Period > 0))
                    {
                        double upperBand, lowerBand;
                        double standardDevUpper, standardDevLower;
                        double M;
                        double S;
                        int k;
                        double tmpM;
                        int subrownum;
                        if (dailyTable.Columns.Contains("Lower Band"))
                        {
                            dailyTable.Columns.Remove("Lower Band");
                        }
                        if (dailyTable.Columns.Contains("Middle Band"))
                        {
                            dailyTable.Columns.Remove("Middle Band");
                        }
                        if (dailyTable.Columns.Contains("Upper Band"))
                        {
                            dailyTable.Columns.Remove("Upper Band");
                        }
                        newCol = new DataColumn("Lower Band", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("Middle Band", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);
                        newCol = new DataColumn("Upper Band", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        for (int rownum = 0; (rownum + 1) < dailyTable.Rows.Count; rownum++)
                        {
                            M = 0.0;
                            S = 0.0;
                            k = 1;
                            //find the standard deviation of price
                            for (subrownum = rownum; ((subrownum < (rownum + small_fast_Period)) && (subrownum < dailyTable.Rows.Count)); subrownum++)
                            {
                                currentClosePrice = System.Convert.ToDouble(dailyTable.Rows[subrownum][seriestype]);
                                //= System.Convert.ToDateTime(dailyTable.Rows[subrownum]["Date"]);

                                tmpM = M;
                                M += (currentClosePrice - tmpM) / k;
                                S += (currentClosePrice - tmpM) * (currentClosePrice - M);
                                k++;
                            }
                            standardDevUpper = Math.Sqrt(S / (k - stddeviation));
                            standardDevLower = Math.Sqrt(S / (k - stddeviation));

                            //get the SMA for the last row date using subrownum

                            //Find upper & lower bands
                            upperBand = System.Convert.ToDouble(dailyTable.Rows[subrownum - 1]["SMA_SMALL"]) + (standardDevUpper * stddeviation);
                            lowerBand = System.Convert.ToDouble(dailyTable.Rows[subrownum - 1]["SMA_SMALL"]) - (standardDevLower * stddeviation);

                            dailyTable.Rows[subrownum - 1]["Lower Band"] = Math.Round(lowerBand, 4);
                            dailyTable.Rows[subrownum - 1]["Middle Band"] = Math.Round(System.Convert.ToDouble(dailyTable.Rows[subrownum - 1]["SMA_SMALL"]), 4);
                            dailyTable.Rows[subrownum - 1]["Upper Band"] = Math.Round(upperBand, 4);
                        }
                    }
                    if (emaRequired == true)
                    {
                        if (dailyTable.Columns.Contains("EMA_SMALL"))
                        {
                            dailyTable.Columns.Remove("EMA_SMALL");
                        }
                        if (dailyTable.Columns.Contains("EMA_LONG"))
                        {
                            dailyTable.Columns.Remove("EMA_LONG");
                        }
                        if (dailyTable.Columns.Contains("EMA_CROSSOVER_FLAG"))
                        {
                            dailyTable.Columns.Remove("EMA_CROSSOVER_FLAG");
                        }
                        newCol = new DataColumn("EMA_SMALL", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("EMA_LONG", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("EMA_CROSSOVER_FLAG", typeof(string));
                        newCol.DefaultValue = "LT";
                        dailyTable.Columns.Add(newCol);


                        double multiplier = 2 / ((double)small_fast_Period + 1);
                        double ema = 0.00;
                        double currentPrice = 0;
                        double prevEMA = 0;

                        if (small_fast_Period > 0)
                        {//we will not have EMA for initial 0 to smallperiod rows. But EMA starts with smallPeriod row = SMA  for that period
                            dailyTable.Rows[small_fast_Period - 1]["EMA_SMALL"] = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[small_fast_Period - 1]["SMA_SMALL"]));
                            //iterate through table starting from smallperiod - 1 ie. if period is 10 then we use SMA for 9th index in table and then start calculating ema
                            //we have already set ema for 9th index above so we can start the loop from 10th index
                            for (int i = small_fast_Period; i < dailyTable.Rows.Count; i++)
                            {
                                //((current CLOSE price - prev ema) & multiplier) + prev ema
                                //ema = ((System.Convert.ToDouble(dailyTable.Rows[i][seriestype]) - emaPrev) * multiplier) + emaPrev;
                                //ema = ((System.Convert.ToDouble(dailyTable.Rows[i][seriestype]) - System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_SMALL"])) * multiplier) + 
                                //    System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_SMALL"]);

                                //Correct formula for EMA:
                                //multiplier = 2 / (time period + 1)
                                //currentEMA = (currentPrice * multiplier) + yesterdayEMA * (1 - multiplier)

                                currentPrice = System.Convert.ToDouble(dailyTable.Rows[i][seriestype]);
                                prevEMA = System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_SMALL"]);

                                ema = currentPrice * multiplier + prevEMA * (1 - multiplier);
                                dailyTable.Rows[i]["EMA_SMALL"] = Math.Round(ema, 2);
                            }
                        }
                        if (long_slow_Period > 0)
                        {

                            dailyTable.Rows[long_slow_Period - 1]["EMA_LONG"] = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[long_slow_Period - 1]["SMA_LONG"]));

                            multiplier = 2 / ((double)long_slow_Period + 1);
                            ema = 0.00;

                            //iterate through table starting from longperiod - 1 ie. if period is 10 then we use SMA for 9th index in table and then start calculating ema
                            //we have already set ema for 9th index above so we can start the loop from 10th index
                            for (int i = long_slow_Period; i < dailyTable.Rows.Count; i++)
                            {
                                //((current CLOSE price - prev ema) & multiplier) + prev ema
                                //ema = ((System.Convert.ToDouble(dailyTable.Rows[i][seriestype]) - emaPrev) * multiplier) + emaPrev;
                                //ema = ((System.Convert.ToDouble(dailyTable.Rows[i][seriestype]) - System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_LONG"])) * multiplier) + System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_LONG"]);

                                //Correct formula for EMA:
                                //multiplier = 2 / (time period + 1)
                                //currentEMA = (currentPrice * multiplier) + yesterdayEMA * (1 - multiplier)

                                currentPrice = System.Convert.ToDouble(dailyTable.Rows[i][seriestype]);
                                prevEMA = System.Convert.ToDouble(dailyTable.Rows[i - 1]["EMA_LONG"]);

                                ema = currentPrice * multiplier + prevEMA * (1 - multiplier);

                                dailyTable.Rows[i]["EMA_LONG"] = Math.Round(ema, 2);

                                dailyTable.Rows[i]["EMA_CROSSOVER_FLAG"] = (System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[i]["EMA_SMALL"])) > Math.Round(ema, 2)) ? "GT" : "LT";
                            }
                        }

                        //find MACD data
                        if (macdRequired && (long_slow_Period > 0) && (small_fast_Period > 0))
                        {
                            //we must have both small & long period
                            double macd = 0.00, signal = 0.00, histogram = 0.00;
                            List<double> listMACD = new List<double>();

                            int emaFastIndex = small_fast_Period;
                            if (dailyTable.Columns.Contains("MACD"))
                            {
                                dailyTable.Columns.Remove("MACD");
                            }
                            if (dailyTable.Columns.Contains("MACD_Hist"))
                            {
                                dailyTable.Columns.Remove("MACD_Hist");
                            }
                            if (dailyTable.Columns.Contains("MACD_Signal"))
                            {
                                dailyTable.Columns.Remove("MACD_Signal");
                            }
                            newCol = new DataColumn("MACD", typeof(decimal));
                            newCol.DefaultValue = 0;
                            dailyTable.Columns.Add(newCol);

                            newCol = new DataColumn("MACD_Hist", typeof(decimal));
                            newCol.DefaultValue = 0;
                            dailyTable.Columns.Add(newCol);

                            newCol = new DataColumn("MACD_Signal", typeof(decimal));
                            newCol.DefaultValue = 0;
                            dailyTable.Columns.Add(newCol);

                            for (int rownum = long_slow_Period; rownum < dailyTable.Rows.Count; rownum++)
                            {
                                //macd = System.Convert.ToDouble(dailyTable.Rows[(emaFastIndex)]["EMA_SMALL"]) - System.Convert.ToDouble(dailyTable.Rows[rownum]["EMA_LONG"]);
                                macd = System.Convert.ToDouble(dailyTable.Rows[rownum]["EMA_SMALL"]) - System.Convert.ToDouble(dailyTable.Rows[rownum]["EMA_LONG"]);
                                listMACD.Add(macd);

                                if (rownum >= ((signalperiod + long_slow_Period) - 1))
                                {
                                    signal = FindSignal(rownum, signalperiod, long_slow_Period, listMACD, signal);
                                    histogram = macd - signal;
                                    dailyTable.Rows[emaFastIndex]["MACD"] = Math.Round(macd, 4);
                                    dailyTable.Rows[emaFastIndex]["MACD_Hist"] = Math.Round(histogram, 4);
                                    dailyTable.Rows[emaFastIndex]["MACD_Signal"] = Math.Round(signal, 4);
                                }
                                emaFastIndex++;
                            }
                            listMACD.Clear();

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetSMATable exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }

        #endregion

        #region UTILITY_METHODS
        public string GetRange(string time_interval, string outputsize)
        {
            StringBuilder range = new StringBuilder();
            if (time_interval.Equals("1d"))
            {
                if (outputsize.Equals("Compact"))
                {
                    range.Append("3mo");
                }
                else //if (outputsize.Equals("compact"))
                {
                    range.Append("10y");
                }
            }
            else if (time_interval == "60m")
            {
                if (outputsize.Equals("Compact"))
                {
                    range.Append("1d");
                }
                else
                {
                    range.Append("2y");
                }

            }
            else if (time_interval == "1m")
            {
                if (outputsize.Equals("Compact"))
                {
                    range.Append("1d");
                }
                else
                {
                    range.Append("7d");
                }
            }
            else //if ((time_interval == "15m") || (time_interval == "30m") || (time_interval == "5m"))
            {
                if (outputsize.Equals("Compact"))
                {
                    range.Append("1d");
                }
                else
                {
                    range.Append("60d");
                }

            }

            return range.ToString();
        }

        public string findTimeZoneId(string zoneId)
        {
            string returnTimeZoneId = "";
            switch (zoneId)
            {
                case "IST":
                    returnTimeZoneId = "India Standard Time";
                    break;
                default:
                    returnTimeZoneId = "India Standard Time";
                    break;
            }
            return returnTimeZoneId;
        }

        public DateTime convertUnixEpochToLocalDateTime(long dateEpoch, string zoneId)
        {
            DateTime localDateTime;

            DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeSeconds(dateEpoch);
            string timeZoneId = findTimeZoneId(zoneId);
            TimeZoneInfo currentTimeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
            localDateTime = TimeZoneInfo.ConvertTimeFromUtc(dateTimeOffset.UtcDateTime, currentTimeZone);

            return localDateTime;
        }

        public double FindTR1(int rownum, DataTable dailyTable)
        {
            //MAX(Current High- Current Low,ABS(Current High- Previous Close),ABS(Current Low - Previous Close))
            double diffHighLow = System.Convert.ToDouble(dailyTable.Rows[rownum]["HIGH"]) - System.Convert.ToDouble(dailyTable.Rows[rownum]["LOW"]);
            double diffCurrHighPrevClose = Math.Abs(System.Convert.ToDouble(dailyTable.Rows[rownum]["HIGH"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["CLOSE"]));
            double diffCurrLowPrevClose = Math.Abs(System.Convert.ToDouble(dailyTable.Rows[rownum]["LOW"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["CLOSE"]));

            double maxTR = Math.Max(diffHighLow, Math.Max(diffCurrHighPrevClose, diffCurrLowPrevClose));

            return maxTR;
        }

        public double FindPositiveDM1(int rownum, DataTable dailyTable)
        {
            //IF((Current High- Previous High)>(Previous Low - Current Low)
            //    MAX((Current High-Previous High),0)
            //ELSE 
            //    0
            double diffCurrHighPrevHigh = System.Convert.ToDouble(dailyTable.Rows[rownum]["HIGH"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["HIGH"]);
            double diffPrevLowCurrLow = System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["LOW"]) - System.Convert.ToDouble(dailyTable.Rows[rownum]["LOW"]);
            double positiveDM1 = 0.00;

            if (diffCurrHighPrevHigh > diffPrevLowCurrLow)
            {
                positiveDM1 = Math.Max(diffCurrHighPrevHigh, 0);
            }

            return positiveDM1;
        }

        public double FindNegativeDM1(int rownum, DataTable dailyTable)
        {
            //IF((Previous Low - Current Low) > (Current High- Previous High))
            //    MAX((Previous Low - Current Low),0)
            //ELSE 
            //    0
            double diffCurrHighPrevHigh = System.Convert.ToDouble(dailyTable.Rows[rownum]["HIGH"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["HIGH"]);
            double diffPrevLowCurrLow = System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["LOW"]) - System.Convert.ToDouble(dailyTable.Rows[rownum]["LOW"]);
            double negativeDM1 = 0.00;

            if (diffPrevLowCurrLow > diffCurrHighPrevHigh)
            {
                negativeDM1 = Math.Max(diffPrevLowCurrLow, 0);
            }

            return negativeDM1;
        }

        /// <summary>
        /// This method shold be called only for rows from "period" onwords
        /// </summary>
        /// <param name="rownum"></param>
        /// <param name="period"></param>
        /// <param name="dailyTable"></param>
        /// <param name="TR1"></param>
        /// <param name="TRPeriod"></param>
        /// <returns></returns>
        public double FindTR_Period(int rownum, int period, List<double> TR1, List<double> TRPeriod)
        {
            double valueTR = 0.00;
            if (rownum == period)
            {
                //SUM of TR1[1] to TR1[14]
                valueTR = TR1.GetRange(0, period).Sum();
            }
            else if (rownum > period)
            {
                //I17-(I17/14)+F18
                //TRPeriod[rownum - 1] - (TRPeriod[rownum - 1]/period) + TR1[rownum]
                valueTR = TRPeriod[rownum - period - 1] - (TRPeriod[rownum - period - 1] / period) + TR1[rownum - 1];
            }
            return valueTR;
        }

        public double FindPositveDM_Period(int rownum, int period, List<double> positiveDM1, List<double> positiveDMPeriod)
        {
            double valueDM = 0.00;
            if (rownum == period)
            {
                //SUM of TR1[1] to TR1[14]
                valueDM = positiveDM1.GetRange(0, period).Sum();
            }
            else if (rownum > period)
            {
                //I17-(I17/14)+F18
                //TRPeriod[rownum - 1] - (TRPeriod[rownum - 1]/period) + TR1[rownum]
                valueDM = positiveDMPeriod[rownum - period - 1] - (positiveDMPeriod[rownum - period - 1] / period) + positiveDM1[rownum - 1];
            }
            return Math.Round(valueDM, 4);
        }

        public double FindNegativeDM_Period(int rownum, int period, List<double> negativeDM1, List<double> negativeDMPeriod)
        {
            double valueDM = 0.00;
            if (rownum == period)
            {
                //SUM of TR1[1] to TR1[14]
                valueDM = negativeDM1.GetRange(0, period).Sum();
            }
            else if (rownum > period)
            {
                //I17-(I17/14)+F18
                //TRPeriod[rownum - 1] - (TRPeriod[rownum - 1]/period) + TR1[rownum]
                valueDM = negativeDMPeriod[rownum - period - 1] - (negativeDMPeriod[rownum - period - 1] / period) + negativeDM1[rownum - 1];
            }
            return Math.Round(valueDM, 4);
        }

        public double FindPositveDI_Period(int rownum, int period, List<double> TRPeriod, List<double> positiveDMPeriod)
        {
            double valueDI;
            //I17-(I17/14)+F18
            //TRPeriod[rownum - 1] - (TRPeriod[rownum - 1]/period) + TR1[rownum]
            valueDI = (100 * ((positiveDMPeriod[rownum - period]) / (TRPeriod[rownum - period])));
            return Math.Round(valueDI, 4);
        }

        public double FindNegativeDI_Period(int rownum, int period, List<double> TRPeriod, List<double> negativeDMPeriod)
        {
            double valueDI;
            //I17-(I17/14)+F18
            //TRPeriod[rownum - 1] - (TRPeriod[rownum - 1]/period) + TR1[rownum]
            valueDI = (100 * ((negativeDMPeriod[rownum - period]) / (TRPeriod[rownum - period])));
            return Math.Round(valueDI, 4);
        }

        public double FindDX(int rownum, int period, List<double> positiveDI, List<double> negativeDI)
        {
            double valueDX;
            double diffDI, sumDI;
            diffDI = Math.Abs(positiveDI[rownum - period] - negativeDI[rownum - period]);
            sumDI = positiveDI[rownum - period] + negativeDI[rownum - period];
            valueDX = 100 * (diffDI / sumDI);
            return Math.Round(valueDX, 4);
        }

        public double FindADX(int rownum, int period, List<double> listDX, List<double> listADX)
        {
            double valueADX = 0.00;

            if (rownum == ((period * 2) - 1))
            //if (rownum == ((period * 2) - 2))
            {
                valueADX = listDX.GetRange(0, period).Average();
            }
            else if (rownum > ((period * 2) - 1))
            //else if (rownum > ((period * 2) - 2))
            {
                valueADX = ((listADX[rownum - (period * 2)] * (period - 1)) + listDX[rownum - period]) / period;
                //valueADX = ((listADX[(rownum + 1) - (period * 2)] * (period - 1)) + listDX[rownum- period]) / period;
            }

            return Math.Round(valueADX, 2);
        }

        public double FindHighestHigh(List<double> listHigh, int start, int count)
        {
            double highestHigh = (listHigh.GetRange(start, count)).Max();
            return highestHigh;
        }
        public double FindLowestLow(List<double> listLow, int start, int count)
        {
            double lowestLow = (listLow.GetRange(start, count)).Min();
            return lowestLow;
        }

        public double FindSlowK(List<double> listClose, List<double> listHighestHigh, List<double> listLowestLow)
        {
            double slowK = ((listClose.Last() - listLowestLow.Last()) / (listHighestHigh.Last() - listLowestLow.Last())) * 100;
            return Math.Round(slowK, 4);
        }

        public double FindSlowD(List<double> listSlowK, int start, int count)
        {
            double slowD = listSlowK.GetRange(start, count).Average();
            return Math.Round(slowD, 4);
        }
        /// <summary>
        /// signal for MACD is 9 period EMA value of MACD
        /// The signal line for the stochastic oscillator is a three-period simple moving average (SMA) of the stochastic (called %K in this case)
        /// </summary>
        /// <param name="rownum"></param>
        /// <param name="signalperiod"></param>
        /// <param name="long_slow_Period"></param>
        /// <param name="listMACD"></param>
        /// <param name="signalPrev"></param>
        /// <returns></returns>
        public double FindSignal(int rownum, int signalperiod, int long_slow_Period, List<double> listMACD, double signalPrev)
        {
            double multiplier = (2 / ((double)signalperiod + 1));
            double signal = 0.00;
            if (rownum == ((signalperiod + long_slow_Period) - 1))
            {
                signal = (listMACD.GetRange(0, signalperiod)).Average();
            }
            else
            {
                //signal = ((listMACD.Last() - signalPrev) * multiplier) + signalPrev;
                signal = listMACD.Last() * multiplier + signalPrev * (1 - multiplier);
            }
            return Math.Round(signal, 4);
        }

        /// <summary>
        /// Given a list of values the method will return Standard Deviation
        /// </summary>
        /// <param name="valueList"></param>
        /// <returns></returns>
        public static double StandardDeviation(List<double> valueList)
        {
            double M = 0.0;
            double S = 0.0;
            int k = 1;
            foreach (double value in valueList)
            {
                double tmpM = M;
                M += (value - tmpM) / k;
                S += (value - tmpM) * (value - M);
                k++;
            }
            return Math.Sqrt(S / (k - 2));
        }

        public double CalculateAroonUp(int rownum, int period, DataTable dailyTable)
        {
            var maxIndex = FindMaxAroonIndex(rownum - period, rownum, dailyTable);

            var up = CalcAroon(rownum - maxIndex, period);

            return Math.Round(up, 4);
        }

        public double CalculateAroonDown(int rownum, int period, DataTable dailyTable)
        {
            var minIndex = FindMinAroonIndex(rownum - period, rownum, dailyTable);

            var down = CalcAroon(rownum - minIndex, period);

            return Math.Round(down, 4);
        }

        public static double CalcAroon(int numOfDays, int period)
        {
            var result = ((period - numOfDays)) * ((double)100 / period);
            return result;
        }

        public int FindMinAroonIndex(int startIndex, int endIndex, DataTable dailyTable)
        {
            var min = double.MaxValue;
            var index = startIndex;
            for (var i = startIndex; i <= endIndex; i++)
            {
                if (min < System.Convert.ToDouble(dailyTable.Rows[i]["LOW"]))
                    continue;

                min = System.Convert.ToDouble(dailyTable.Rows[i]["LOW"]);
                index = i;
            }
            return index;
        }

        public int FindMaxAroonIndex(int startIndex, int endIndex, DataTable dailyTable)
        {
            var max = double.MinValue;
            var index = startIndex;
            for (var i = startIndex; i <= endIndex; i++)
            {
                if (max > System.Convert.ToDouble(dailyTable.Rows[i]["HIGH"]))
                    continue;

                max = System.Convert.ToDouble(dailyTable.Rows[i]["HIGH"]);
                index = i;
            }
            return index;
        }

        #endregion
        public void TestLoadFromTo(DateTime dateFrom, DateTime dateTo)
        {
            string fromDt = dateFrom.ToString("yyyy-MM-dd");
            string toDt = dateTo.ToString("yyyy-MM-dd");
            DataTable fundhouseTable = getFundHouseTable();
            //foreach (DataRow row in fundhouseTable.Rows)
            for (int rownum = 1; rownum < fundhouseTable.Rows.Count; rownum++)
            {
                DataRow row = fundhouseTable.Rows[rownum];
                //if (row["FUNDHOUSECODE"].ToString().Equals("-1") == false)
                {
                    getHistoryNAVForMFCode(row["FUNDHOUSECODE"].ToString(), fromdt: fromDt, todt: toDt);
                }
            }
        }

        public void UpdateNAVFromLastFetchDate(int fundhousecode = -1)
        {
            DataTable fundhouseTable = getFundHouseTable(fundhousecode);
            DateTime dateLastUpdate;
            foreach (DataRow fundRow in fundhouseTable.Rows)
            {
                dateLastUpdate = System.Convert.ToDateTime(fundRow["LAST_UPDATE_DATE"].ToString());
                if (dateLastUpdate.Equals(DateTime.MinValue))
                {
                    dateLastUpdate = System.Convert.ToDateTime("2008-01-01");
                }
                if (dateLastUpdate < DateTime.Today)
                {

                    getHistoryNAVForMFCode(fundRow["FUNDHOUSECODE"].ToString(), fromdt: dateLastUpdate.AddDays(1).ToString("yyyy-MM-dd"), todt: DateTime.Today.ToString("yyyy-MM-dd"));
                }
            }
        }
    }
}
