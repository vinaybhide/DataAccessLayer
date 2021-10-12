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
        static string urlMF_NAV_FOR_DATE = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={0}";


        //Use following URL to get NAV history between from dt & to dt for specific MF code. 
        //Output is :
        //Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date
        //http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=27&frmdt=27-Sep-2020&todt=05-Oct-2020
        static string urlMF_NAV_HISTORY_FROM_TO = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&frmdt={1}&todt={2}";
        static string urlMF_NAV_HISTORY_FROM = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={0}&frmdt={1}";


        static string dbFile = ".\\portfolio\\MFData\\mfdata.db";
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

        static public SQLiteConnection CreateConnection()
        {
            SQLiteConnection sqlite_conn = null;
            string sCurrentDir = AppDomain.CurrentDomain.BaseDirectory;
            string sFile = System.IO.Path.Combine(sCurrentDir, dbFile);
            string sFilePath = Path.GetFullPath(sFile);


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

        static public void ReadData(SQLiteConnection conn)
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
        static public bool getAllMFNAVToday()
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
        static public bool getAllMFNAVForDate(string fetchDate)
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
        static public bool getHistoryNAVForMFCode(string mfCode, string fromdt, string todt = null)
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
        #endregion

        #region insert methods

        /// <summary>
        /// method that inserts data in DB. Inserts recrods in FUNDHOUSE, SCHEME_TYPE, NAVRECORDS
        /// It receives a stringbuilder object containing data for all schemes including fundhouse & scheme types and nav date & nav for the specific date
        /// </summary>
        /// <param name="sourceFile">A string builder object containing all records read from respective URL</param>
        /// <returns>Number of recrods processed</returns>
        static public long insertRecordInDB(StringBuilder sourceFile)
        {
            string[] fields;
            StringBuilder record = new StringBuilder(string.Empty);
            //DataRow r;
            StringBuilder mfType = new StringBuilder(string.Empty);
            StringBuilder tmp1 = new StringBuilder(string.Empty);
            StringBuilder mfCompName = new StringBuilder(string.Empty);
            int fundhousecode = -1;
            long schemetypeid = -1;
            int schemecode = -1;
            int recCounter = 0;
            string[] sourceLines;
            double nav;
            StringBuilder schemeName = new StringBuilder(string.Empty), ISINDivPayoutISINGrowth = new StringBuilder(string.Empty), ISINDivReinvestment = new StringBuilder(string.Empty);
            StringBuilder netAssetValue = new StringBuilder(string.Empty), navDate = new StringBuilder(string.Empty);
            StringBuilder recFormat1 = new StringBuilder("Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date");
            StringBuilder recFormat2 = new StringBuilder("Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date");
            StringBuilder sCurrentSchemeName = new StringBuilder(string.Empty);
            StringBuilder sCurrentFundHouse = new StringBuilder(string.Empty);
            DateTime dateStart = DateTime.Today, dateEnd = DateTime.Today;
            DateTime dateNAV;
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

                                    schemetypeid = getSchemeTypeId(mfType.ToString(), sqlite_cmd);
                                    if (schemetypeid == -1)
                                    {
                                        schemetypeid = insertSchemeType(mfType.ToString(), sqlite_cmd);
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

                                    if ((sCurrentFundHouse.Equals(string.Empty)) || (sCurrentFundHouse.Equals(mfCompName) == false))
                                    {
                                        fundhousecode = getFundHouseCode(mfCompName.ToString(), sqlite_cmd);
                                        if (fundhousecode == -1)
                                        {
                                            //this should never happen as the fundhouse table is manually maintained
                                            fundhousecode = insertFundHouse(mfCompName.ToString(), sqlite_cmd);
                                        }
                                        sCurrentFundHouse.Clear();
                                        sCurrentFundHouse.Append(mfCompName);
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
                                Console.WriteLine("insertRecordInDB exception while setting NAV value: " + ex.Message);
                                nav = 0.00;
                            }
                            if (nav == 0)
                            {
                                continue;
                            }
                            //first get the schemecode
                            schemecode = int.Parse(fields[0]);
                            ISINDivPayoutISINGrowth.Clear();
                            ISINDivPayoutISINGrowth.Append(fields[1]); ;
                            ISINDivReinvestment.Clear();
                            ISINDivReinvestment.Append(fields[2]);
                            schemeName.Clear();
                            schemeName.Append(fields[3]);
                            navDate.Clear();
                            navDate.Append(fields[5]);

                            if (fields.Length == 8)
                            {
                                schemeName.Clear();
                                schemeName.Append(fields[1]);
                                ISINDivPayoutISINGrowth.Clear();
                                ISINDivPayoutISINGrowth.Append(fields[2]);
                                ISINDivReinvestment.Clear();
                                ISINDivReinvestment.Append(fields[3]);
                                navDate.Clear();
                                navDate.Append(fields[7]);
                            }

                            dateNAV = System.Convert.ToDateTime(navDate.ToString());
                            //Now check if scheme exists in SCHEMES table
                            if ((sCurrentSchemeName.Equals(string.Empty)) ||
                               (sCurrentSchemeName.Equals(schemeName) == false) ||
                               (isSchemeExists(schemecode, sqlite_cmd).ToUpper().Equals(schemeName.ToString().ToUpper()) == false))
                            //(schemeName.Equals(isSchemeExists(schemecode, sqlite_cmd)) == false))
                            {
                                //insert new scheme in schemes tables
                                insertScheme(fundhousecode, schemetypeid, schemecode, schemeName.ToString(), dateNAV.ToString("yyyy-MM-dd"), dateNAV.ToString("yyyy-MM-dd"), sqlite_cmd);
                                //System.Convert.ToDateTime(navDate).ToString("yyyy-MM-dd"), System.Convert.ToDateTime(navDate).ToString("yyyy-MM-dd"));
                                sCurrentSchemeName = schemeName;
                                dateStart = dateNAV;
                                dateEnd = dateNAV;
                                //Console.WriteLine("Counter= " + recCounter + "Scheme code= " + schemecode + "--Scheme name= " + schemeName);
                            }

                            //MF_TYPE;MF_COMP_NAME;SCHEME_CODE;ISIN_Div_Payout_ISIN_Growth;ISIN_Div_Reinvestment;SCHEME_NAME;NET_ASSET_VALUE;DATE
                            insertTransaction(schemecode, ISINDivPayoutISINGrowth.ToString(), ISINDivReinvestment.ToString(), schemeName.ToString(),
                                        string.Format("{0:0.0000}", nav), dateNAV.ToString("yyyy-MM-dd"), sqlite_cmd);
                        }
                    }
                    UpdateSchemeFromToDate(sqlite_cmd);
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

        static public long insertSchemeType(string schemeType, SQLiteCommand sqlite_cmd)
        {
            long schemetypeid = -1;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "REPLACE INTO  SCHEME_TYPE(TYPE) VALUES (@TYPE)";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@TYPE", schemeType);

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
                    schemetypeid = getSchemeTypeId(schemeType, sqlite_cmd);
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

        static public int insertFundHouse(string fundHouse, SQLiteCommand sqlite_cmd)
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
                        sqlite_cmd.CommandText = "INSERT INTO FUNDHOUSE(FUNDHOUSECODE, NAME) VALUES (@CODE, @NAME)";
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

        static public int insertScheme(int fundHouseCode, long schemeTypeId, int schemeCode, string schemeName, string dateFrom, string dateTo, SQLiteCommand sqlite_cmd)
        {
            int numOfRowsInserted = 0;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                //sqlite_cmd.CommandText = "REPLACE INTO SCHEMES(SCHEMECODE, SCHEMENAME, FUNDHOUSECODE, SCHEMETYPEID) VALUES (@SCHEMECODE, @SCHEMENAME, @FUNDHOUSECODE, @SCHEMETYPEID)";
                sqlite_cmd.CommandText = "REPLACE INTO  SCHEMES(SCHEMECODE, SCHEMENAME, FUNDHOUSECODE, SCHEMETYPEID, FROM_DATE, TO_DATE) " +
                                         "VALUES (@SCHEMECODE, @SCHEMENAME, @FUNDHOUSECODE, @SCHEMETYPEID, @FROM_DATE, @TO_DATE)";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMENAME", schemeName);
                sqlite_cmd.Parameters.AddWithValue("@FUNDHOUSECODE", fundHouseCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMETYPEID", schemeTypeId);
                sqlite_cmd.Parameters.AddWithValue("@FROM_DATE", dateFrom);
                sqlite_cmd.Parameters.AddWithValue("@TO_DATE", dateTo);

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
                Console.WriteLine("insertScheme: [" + fundHouseCode + "," + schemeTypeId + "," + schemeCode + "," + schemeName + "] " + ex.Message);
            }
            //sqlite_conn = null;
            //sqlite_cmd = null;
            return numOfRowsInserted;
        }

        //Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date
        //insertTransaction(fundhousecode, schemetypeid, schemecode, fields[1], fields[2], fields[3], string.Format("{0:0.0000}", nav), System.Convert.ToDateTime(fields[5]).ToString("yyyy-MM-dd"));
        static public int insertTransaction(int schemeCode, string ISINDivPayout_ISINGrowth, string ISINDivReinvestment,
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
                sqlite_cmd.CommandText = "REPLACE INTO NAVRECORDS(SCHEMECODE, ISIN_Div_Payout_ISIN_Growth, ISIN_Div_Reinvestment, " +
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

        static public int UpdateSchemeFromToDate(SQLiteCommand sqlite_cmd)
        {
            int numOfRowsUpdated = 0;
            //SQLiteConnection sqlite_conn = null;
            //SQLiteCommand sqlite_cmd = null;
            try
            {
                //sqlite_conn = CreateConnection();
                //sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "UPDATE SCHEMES SET FROM_DATE = T.MINNAVDATE, TO_DATE = T.MAXNAVDATE " +
                                         "FROM (select MIN(NAVDATE) AS MINNAVDATE, MAX(NAVDATE) AS MAXNAVDATE, SCHEMECODE from NAVRECORDS GROUP BY 3) AS T " +
                                         "WHERE SCHEMES.SCHEMECODE = T.SCHEMECODE";
                sqlite_cmd.Prepare();
                //sqlite_cmd.Parameters.AddWithValue("@CODE1", schemeCode);
                //sqlite_cmd.Parameters.AddWithValue("@CODE2", schemeCode);
                try
                {
                    numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("UpdateSchemeFromToDate: " + exSQL.Message);
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
                Console.WriteLine("insertScheme: " + ex.Message);
            }
            //sqlite_conn = null;
            //sqlite_cmd = null;
            return numOfRowsUpdated;
        }

        static public int updateSchemeFromDate(int schemeCode, string schemeName, string dateFrom)
        {
            int numOfRowsUpdated = 0;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "UPDATE SCHEMES SET FROM_DATE = @FROM_DATE WHERE SCHEMECODE = @SCHEMECODE AND SCHEMENAME = @SCHEMENAME";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@FROM_DATE", dateFrom);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMENAME", schemeName);
                try
                {
                    numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("updateSchemeFromDate: [" + schemeCode + "," + schemeName + "," + dateFrom + "] " + exSQL.Message);
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
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertScheme: [" + schemeCode + "," + schemeName + "] " + ex.Message);
            }
            sqlite_conn = null;
            sqlite_cmd = null;
            return numOfRowsUpdated;
        }

        static public int updateSchemeToDate(int schemeCode, string schemeName, string dateTo)
        {
            int numOfRowsUpdated = 0;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                sqlite_cmd.CommandText = "UPDATE SCHEMES SET TO_DATE = @TO_DATE WHERE SCHEMECODE = @SCHEMECODE AND SCHEMENAME = @SCHEMENAME";
                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@TO_DATE", dateTo);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                sqlite_cmd.Parameters.AddWithValue("@SCHEMENAME", schemeName);
                try
                {
                    numOfRowsUpdated = sqlite_cmd.ExecuteNonQuery();
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("updateSchemeFromDate: [" + schemeCode + "," + schemeName + "," + dateTo + "] " + exSQL.Message);
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
            }
            catch (Exception ex)
            {
                Console.WriteLine("insertScheme: [" + schemeCode + "," + schemeName + "] " + ex.Message);
            }
            sqlite_conn = null;
            sqlite_cmd = null;
            return numOfRowsUpdated;
        }

        #endregion
        #region get_methods
        /// <summary>
        /// Method to get predefined fundhouse code for given fundhouse/mf company value
        /// </summary>
        /// <param name="fundHouse">name of the fund house</param>
        /// <returns>matchng fund house code</returns>
        static public int getFundHouseCode(string fundHouse, SQLiteCommand sqlite_cmd = null)
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
        static public DataTable getFundHouseTable()
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
                sqlite_cmd.CommandText = "SELECT FUNDHOUSECODE, NAME FROM FUNDHOUSE";
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
        static public long getSchemeTypeId(string schemeType, SQLiteCommand sqlite_cmd)
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
        static public DataTable getSchemeTypeTable()
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
                sqlite_cmd.CommandText = "SELECT ROWID, TYPE FROM SCHEME_TYPE";
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
        /// <returns>matching scheme name if found else empty string </returns>
        static public string isSchemeExists(int schemeCode, SQLiteCommand sqlite_cmd)
        {
            string schemeName = string.Empty;
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
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        schemeName = sqlite_datareader["SCHEMENAME"].ToString();
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("isSchemeExists: [" + schemeCode + "] " + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("isSchemeExists: [" + schemeCode + "] " + ex.Message);
                schemeName = string.Empty;
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
            return schemeName;
        }

        /// <summary>
        /// returns data from schemes table matching given fundhouse code and scheme type id. The filters are ignored if value passed to method = -1
        /// </summary>
        /// <param name="fundhousecode"></param>
        /// <param name="schemetypeid"></param>
        /// <returns>Data Table matching criterion provided in fundhousecode and schemetypeid</returns>
        static public DataTable getSchemesTable(int fundhousecode = -1, int schemetypeid = -1)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;


            string statement = "SELECT SCHEME_TYPE.ROWID AS SCHEMETYPEID , SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, SCHEMES.SCHEMECODE, SCHEMES.SCHEMENAME, " +
                                "SCHEMES.FROM_DATE, SCHEMES.TO_DATE FROM SCHEMES " +
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

        static public int getMaxFundHouseID(string fundHouse, SQLiteCommand sqlite_cmd)
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

        static public DataTable getNAVRecordsTable(int schemecode, string fromDate = null, string toDate = null)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement = "SELECT NAVRECORDS.ROWID AS ID, SCHEME_TYPE.ROWID as SCHEMETYPEID, SCHEME_TYPE.TYPE, FUNDHOUSE.FUNDHOUSECODE, FUNDHOUSE.NAME, SCHEMES.SCHEMECODE, " +
                               "SCHEMES.SCHEMENAME, NAVRECORDS.NET_ASSET_VALUE, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDATE  from SCHEMES " +
                               "INNER JOIN SCHEME_TYPE ON SCHEMES.SCHEMETYPEID = SCHEME_TYPE.ROWID " +
                               "INNER JOIN FUNDHOUSE ON SCHEMES.FUNDHOUSECODE = FUNDHOUSE.FUNDHOUSECODE " +
                               "INNER JOIN NAVRECORDS ON SCHEMES.SCHEMECODE = NAVRECORDS.SCHEMECODE ";
            try
            {
                statement += "WHERE SCHEMES.SCHEMECODE = " + schemecode.ToString() + " ";

                if (fromDate != null)
                {
                    statement += "AND date(NAVRECORDS.NAVDATE) >= date(\"" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd") + "\") ";
                }

                if (toDate != null)
                {
                    statement += "AND date(NAVRECORDS.NAVDATE) <= date(\"" + System.Convert.ToDateTime(toDate).ToString("yyyy-MM-dd") + "\") ";
                }
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = statement;
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
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

        static public DataTable getRSIDataTableFromDailyNAV(int schemecode = -1, string fromDate = null, string toDate = null, string period = "20")
        {
            DataTable dailyTable = null;
            DataTable rsiDataTable = null;
            int iPeriod;
            double change, gain, loss, avgGain = 0.00, avgLoss = 0.00, rs, rsi;
            double sumOfGain = 0.00, sumOfLoss = 0.00;
            DateTime dateCurrentRow = DateTime.Today;

            try
            {
                dailyTable = getNAVRecordsTable(schemecode, fromDate, toDate);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    iPeriod = System.Convert.ToInt32(period);
                    rsiDataTable = new DataTable();

                    rsiDataTable.Columns.Add("SCHEMECODE", typeof(long));
                    rsiDataTable.Columns.Add("SCHEMENAME", typeof(string));
                    rsiDataTable.Columns.Add("Date", typeof(DateTime));
                    rsiDataTable.Columns.Add("RSI", typeof(decimal));

                    //Strat from 1st row in dailyTable and sum all the "seriestype" column upto "period"
                    //SMA = divide the sum by "period"
                    //Store the symbol, Date from the last row of the current set and SMA in the smaDataTable

                    for (int rownum = 1; rownum < dailyTable.Rows.Count; rownum++)
                    {
                        //current - prev
                        change = System.Convert.ToDouble(dailyTable.Rows[rownum]["NET_ASSET_VALUE"]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1]["NET_ASSET_VALUE"]);
                        dateCurrentRow = System.Convert.ToDateTime(dailyTable.Rows[rownum]["NAVDATE"]);

                        if (change < 0)
                        {
                            loss = change;
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
                        else if (rownum == iPeriod)
                        {
                            sumOfGain += gain;
                            sumOfLoss += loss;
                            //we also find  other fields and SAVE
                            avgGain = sumOfGain / iPeriod;
                            avgLoss = sumOfLoss / iPeriod;
                            rs = avgGain / avgLoss;
                            rsi = 100 - (100 / (1 - rs));
                            rsiDataTable.Rows.Add(new object[] {
                                                                    schemecode,
                                                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
                                                                    dateCurrentRow.ToString("dd-MM-yyyy"),
                                                                    Math.Round(rsi, 4)
                                                                });
                        }
                        else
                        {
                            avgGain = ((avgGain * (iPeriod - 1)) + gain) / iPeriod;
                            avgLoss = ((avgLoss * (iPeriod - 1)) + loss) / iPeriod;
                            rs = avgGain / avgLoss;
                            rsi = 100 - (100 / (1 - rs));
                            rsiDataTable.Rows.Add(new object[] {
                                                                    schemecode,
                                                                    dailyTable.Rows[rownum]["SCHEMENAME"].ToString(),
                                                                    dateCurrentRow.ToString("dd-MM-yyyy"),
                                                                    Math.Round(rsi, 4)
                                                                });
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getRSIDataTableFromDailyNAV exception: " + ex.Message);

                if (rsiDataTable != null)
                {
                    rsiDataTable.Clear();
                    rsiDataTable.Dispose();
                }
                rsiDataTable = null;
            }
            if (dailyTable != null)
            {
                dailyTable.Clear();
                dailyTable.Dispose();
            }
            dailyTable = null;
            return rsiDataTable;
        }
        #endregion

        #region portfolio
        static public long createnewMFPortfolio(string userid, string portfolioname)
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

        static public DataTable getPortfolioTable(string userId, SQLiteCommand sqlite_cmd = null)
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
        static public long getPortfolioId(string portfolioName, string userId, SQLiteCommand sqlite_cmd = null)
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

        static public bool addNewTransaction(string userId, string portfolioName, string schemeCode, string purchaseDate, string purchaseNAV,
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

        static public int getNextSIPDurationCounter(string frequency)
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

        static public bool addNewSIP(string userId, string portfolioName, long portfolioRowId, string schemeCode, string startDate, string endDate, string monthlyContribution,
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

                        for (int rownum = 0; rownum < datewiseData.Rows.Count; rownum += getNextSIPDurationCounter(sipFrequency))
                        {
                            purchaseNAV = System.Convert.ToDouble(datewiseData.Rows[rownum]["NET_ASSET_VALUE"]);

                            purchaseUnits = 0.00;
                            if (purchaseNAV > 0)
                            {
                                purchaseUnits = Math.Round((valueAtCost / purchaseNAV), 4);
                            }
                            //string.Format("{0:0.0000}", fields[4])
                            addNewTransaction(userId, portfolioName, schemeCode,
                                datewiseData.Rows[rownum]["NAVDATE"].ToString(),
                                string.Format("{0:0.0000}", purchaseNAV),
                                string.Format("{0:0.0000}", purchaseUnits), string.Format("{0:0.0000}", valueAtCost), portfolioRowId, sqlite_cmd: sqlite_cmd);
                        }
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

        static public bool deletePortfolioRow(string userId, string portfolioName, string portfolioRowId, string schemeCode, string purchaseDate, string purchaseNAV, string purchaseUnits, string valueAtCost)
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
                //portfolioId = System.Convert.ToInt64(portfolioRowId);
                //if (portfolioId > 0)
                {
                    sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE ROWID = " + portfolioRowId;

                    //sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE MASTER_ROWID = @MASTER_ROW_ID AND SCHEMECODE = @SCHEMECODE AND PURCHASE_DATE = @PURCHASE_DATE AND PURCHASE_NAV = @PURCHASE_NAV " +
                    //                        "AND PURCHASE_UNITS = @PURCHASE_UNITS AND VALUE_AT_COST = @VALUE_AT_COST";

                    //sqlite_cmd.Prepare();
                    //sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", portfolioId);
                    //sqlite_cmd.Parameters.AddWithValue("@SCHEMECODE", schemeCode);
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(purchaseDate).ToString("yyyy-MM-dd"));
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_NAV", string.Format("{0:0:0000}", purchaseNAV));
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_UNITS", string.Format("{0:0.0000}", purchaseUnits));
                    //sqlite_cmd.Parameters.AddWithValue("@VALUE_AT_COST", string.Format("{0:0.0000}", valueAtCost));
                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            breturn = true;
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioName + "] " + exSQL.Message);
                    }
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

        static public bool updateTransaction(string userId, string portfolioName, string portfolioRowId, string oldschemeCode, string oldpurchaseDate, string oldpurchaseNAV, string oldpurchaseUnits, string oldvalueAtCost,
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
                        Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioName + "] " + exSQL.Message);
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


        static public bool deletePortfolio(string userId, string portfolioName)
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

                portfolioId = getPortfolioId(portfolioName, userId, sqlite_cmd);
                if (portfolioId > 0)
                {
                    sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE MASTER_ROWID = @MASTER_ROW_ID";
                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", portfolioId);
                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            sqlite_cmd.CommandText = "DELETE FROM PORTFOLIO WHERE MASTER_ROWID = @MASTER_ROW_ID";
                            sqlite_cmd.Prepare();
                            sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", portfolioId);
                            if (sqlite_cmd.ExecuteNonQuery() > 0)
                            {
                                breturn = true;
                            }
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioName + "] " + exSQL.Message);
                    }
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch (Exception ex)
            {
                Console.WriteLine("deletePortfolio: [" + userId + "," + portfolioName + "] " + ex.Message);
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

        static public DataTable openMFPortfolio(string userId, string portfolioFileName, string portfolioRowId, bool bCurrent = true, bool bValuation = false)
        {
            DataTable resultDataTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement = "SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.NAME as FundHouse, SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, " +
                               "strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE) AS PurchaseDate, PORTFOLIO.PURCHASE_NAV as PurchaseNAV, PORTFOLIO.PURCHASE_UNITS as PurchaseUnits, " +
                               "PORTFOLIO.VALUE_AT_COST as ValueAtCost, NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate from SCHEMES " +
                               "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
                               "INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "WHERE NAVRECORDS.NAVDATE = SCHEMES.TO_DATE AND PORTFOLIO.MASTER_ROWID = " + portfolioRowId;

            try
            {

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
                    resultDataTable.Columns.Add("FundHouse", typeof(string)); //FundHouse
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

                    ////resultDataTable.Columns.Add("CurrentValue", typeof(decimal), "Math.Round(CurrentNAV * System.Convert.ToDouble(string.Format(\"{0:0.0000}\", PurchaseUnits)), 4)");
                    //resultDataTable.Columns.Add("CurrentValue", typeof(decimal), "(CurrentNAV * System.Convert.ToDouble(string.Format(\"{0:0.0000}\", PurchaseUnits))");

                    ////resultDataTable.Columns.Add("YearsInvested", typeof(decimal), "Math.Round(((NAVDate - PurchaseDate).TotalDays) / 365.25, 4)");
                    //resultDataTable.Columns.Add("YearsInvested", typeof(decimal), "(((NAVDate - PurchaseDate).TotalDays) / 365.25)");

                    ////resultDataTable.Columns.Add("ARR", typeof(decimal), "Math.Round(Math.Pow((CurrentValue / System.Convert.ToDouble(string.Format(\"{0:0.0000}\", ValueAtCost))), (1 / YearsInvested)) - 1, 4)");
                    //resultDataTable.Columns.Add("ARR", typeof(decimal), "(Math.Pow((CurrentValue / System.Convert.ToDouble(string.Format(\"{0:0.0000}\", ValueAtCost))), (1 / YearsInvested)) - 1)");

                    resultDataTable.Load(sqlite_datareader);
                    resultDataTable.RowChanged -= new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
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

        private static void handlerPortfolioTableRowChanged(object sender, DataRowChangeEventArgs e)
        {
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);

            double currentNAV, valueAtCost, currentValue, yearsInvested, arr;
            DateTime currentNAVdt, purchaseNAVDt;
            currentNAV = 0.00; currentValue = 0.00; yearsInvested = 0.00; arr = 0.00; valueAtCost = 0.00;
            //currentNAVdt = DateTime.Today;
            currentNAVdt = DateTime.MinValue;

            currentNAV = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["CurrentNAV"]));
            currentValue = Math.Round(currentNAV * System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["PurchaseUnits"])), 4);
            currentNAVdt = System.Convert.ToDateTime(e.Row["NAVDate"].ToString());
            purchaseNAVDt = System.Convert.ToDateTime(e.Row["PurchaseDate"].ToString());

            try
            {
                yearsInvested = Math.Round(((currentNAVdt - purchaseNAVDt).TotalDays) / 365.25, 4);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                yearsInvested = Math.Round(0.00, 4);
            }
            valueAtCost = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["ValueAtCost"]));

            try
            {
                arr = Math.Round(0.00, 4);
                if (yearsInvested > 0)
                {
                    arr = Math.Round(Math.Pow((currentValue / valueAtCost), (1 / yearsInvested)) - 1, 4);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                arr = Math.Round(0.00, 4);
            }
            e.Row["PurchaseDate"] = purchaseNAVDt.ToString("yyyy-MM-dd");
            e.Row["PurchaseNAV"] = string.Format("{0:0.0000}", e.Row["PurchaseNAV"]);
            e.Row["PurchaseUnits"] = string.Format("{0:0.0000}", e.Row["PurchaseUnits"]);
            e.Row["ValueAtCost"] = string.Format("{0:0.0000}", valueAtCost);

            e.Row["CurrentNAV"] = string.Format("{0:0.0000}", currentNAV);
            e.Row["NAVDate"] = currentNAVdt.ToString("yyyy-MM-dd");
            e.Row["CurrentValue"] = string.Format("{0:0.0000}", currentValue);
            e.Row["YearsInvested"] = string.Format("{0:0.0000}", yearsInvested);
            e.Row["ARR"] = string.Format("{0:0.0000}", arr);

            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerPortfolioTableRowChanged);
        }

        public static DataTable GetMFValuationBarGraph(string userId, string portfolioName, string portfolioId)
        {
            DataTable resultDataTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement = "SELECT PORTFOLIO.ROWID AS ID, FUNDHOUSE.NAME AS FundHouse, SCHEMES.SCHEMENAME as FundName, SCHEMES.SCHEMECODE as SCHEME_CODE, " +
                               "min(strftime('%d-%m-%Y', PORTFOLIO.PURCHASE_DATE)) as FirstPurchaseDate, " +
                               "sum(PORTFOLIO.PURCHASE_UNITS) as CumulativeUnits, " +
                               "sum(PORTFOLIO.VALUE_AT_COST) as CumulativeCost, " +
                               "NAVRECORDS.NET_ASSET_VALUE AS CurrentNAV, " +
                               "strftime('%d-%m-%Y', NAVRECORDS.NAVDATE) as NAVDate, " +
                               "(sum(PORTFOLIO.PURCHASE_UNITS) * NAVRECORDS.NET_ASSET_VALUE) as CumulativeValue " +
                               "from SCHEMES " +
                               "INNER JOIN PORTFOLIO ON PORTFOLIO.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "INNER JOIN FUNDHOUSE ON FUNDHOUSE.FUNDHOUSECODE = SCHEMES.FUNDHOUSECODE " +
                               "INNER JOIN NAVRECORDS ON NAVRECORDS.SCHEMECODE = SCHEMES.SCHEMECODE " +
                               "WHERE NAVRECORDS.NAVDATE = SCHEMES.TO_DATE AND PORTFOLIO.MASTER_ROWID = " + portfolioId +
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

        private static void handlerMFValuationBarGraphRowChanged(object sender, DataRowChangeEventArgs e)
        {
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);

            double currentNAV, valueAtCost, currentValue, yearsInvested, arr;
            DateTime currentNAVdt, firstPurchaseNAVDt;
            currentNAV = 0.00; currentValue = 0.00; yearsInvested = 0.00; arr = 0.00; valueAtCost = 0.00;
            //currentNAVdt = DateTime.Today;
            currentNAVdt = DateTime.MinValue;
            //string fundName = e.Row["FundName"].ToString().Replace("'", "\'");

            currentNAV = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["CurrentNAV"]));
            currentNAVdt = System.Convert.ToDateTime(e.Row["NAVDate"].ToString());
            firstPurchaseNAVDt = System.Convert.ToDateTime(e.Row["FirstPurchaseDate"].ToString());

            currentValue = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["CumulativeValue"]));
            valueAtCost = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["CumulativeCost"]));

            try
            {
                yearsInvested = Math.Round(((currentNAVdt - firstPurchaseNAVDt).TotalDays) / 365.25, 4);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                yearsInvested = Math.Round(0.00, 4);
            }

            try
            {
                arr = Math.Round(0.00, 4);
                if (yearsInvested > 0)
                {
                    arr = Math.Round(Math.Pow((currentValue / valueAtCost), (1 / yearsInvested)) - 1, 4);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                arr = Math.Round(0.00, 4);
            }
            //e.Row["FundName"] = fundName;
            e.Row["TotalYearsInvested"] = string.Format("{0:0.0000}", yearsInvested);
            e.Row["TotalARR"] = string.Format("{0:0.0000}", arr);

            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerMFValuationBarGraphRowChanged);
        }

        public static DataTable GetValuationLineGraph(string userId, string portfolioName, string portfolioId)
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
                    "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioId +
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
                                    "WHERE PORTFOLIO.MASTER_ROWID = " + portfolioId + " AND PORTFOLIO.SCHEMECODE = " + summaryRow["SCHEMECODE"] +
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
                            while((navRowNum < navTable.Rows.Count) && ((navDate = System.Convert.ToDateTime(navTable.Rows[navRowNum]["DATE"].ToString())) < nextTxnDate))
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

        static public void TestLoadFromTo(DateTime dateFrom, DateTime dateTo)
        {
            string fromDt = dateFrom.ToString("yyyy-MM-dd");
            string toDt = dateTo.ToString("yyyy-MM-dd");
            DataTable fundhouseTable = getFundHouseTable();
            //foreach (DataRow row in fundhouseTable.Rows)
            for (int rownum = 33; rownum < fundhouseTable.Rows.Count; rownum++)
            {
                DataRow row = fundhouseTable.Rows[rownum];
                //if (row["FUNDHOUSECODE"].ToString().Equals("-1") == false)
                {
                    getHistoryNAVForMFCode(row["FUNDHOUSECODE"].ToString(), fromdt: fromDt, todt: toDt);
                }
            }
        }
    }
}
