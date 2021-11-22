using Newtonsoft.Json;
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
    public class StockManager
    {
        static string dbFile = "mfdata.db";

        public static string indicators = "quote";
        public static string includeTimestamps = "true";

        //following link gives a CSV list of all stocks listed on NSE in following format
        //SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE
        //First line is list of fields
        static string urlNSEStockMaster = "http://www1.nseindia.com/content/equities/EQUITY_L.csv";


        //https://query1.finance.yahoo.com/v7/finance/chart/HDFC.BO?range=2yr&interval=1d&indicators=quote&includeTimestamps=true
        //https://query1.finance.yahoo.com/v7/finance/chart/HDFC.BO?range=2yr&interval=1d&indicators=quote&includeTimestamps=true
        public static string urlGetStockData = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?range={1}&interval={2}&indicators={3}&includeTimestamps={4}";

        //https://query1.finance.yahoo.com/v8/finance/chart/HDFC.BO?range=1d&interval=1d&indicators=quote&timestamp=true
        public static string urlGlobalQuote = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?range=1d&interval=1d&indicators=quote&timestamp=true";

        #region online menthods

        /// <summary>
        /// Method to fetch stock master data from NSE and then store in SQLite table
        /// The url returns comma separated recrods with first record is the field descriptor as shown below
        /// SYMBOL,NAME OF COMPANY, SERIES, DATE OF LISTING, PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE
        /// 
        /// We will read each line, then split the fields and then store each record in DB
        /// </summary>
        /// <param name="exchangeCode"></param>
        /// <returns>true if data is stored else fals</returns>
        public int FetchStockMasterFromWebAndInsert(string exchangeCode = "NS")
        {
            int numOfRowsInserted = 0;
            string webservice_url;
            Uri url;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            StringBuilder sourceFile;
            string[] fields;
            StringBuilder record = new StringBuilder(string.Empty);
            string[] sourceLines;
            int recCounter = 0;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;
            string dateToday = DateTime.Today.ToString("yyyy-MM-dd");
            try
            {
                if (exchangeCode.Equals("NS"))
                {
                    webservice_url = urlNSEStockMaster;
                    url = new Uri(webservice_url);
                    var webRequest = WebRequest.Create(url);
                    webRequest.Method = WebRequestMethods.File.DownloadFile;
                    //webRequest.ContentType = "application/json";
                    wr = webRequest.GetResponseAsync().Result;
                    receiveStream = wr.GetResponseStream();
                    reader = new StreamReader(receiveStream);
                }
                else if (exchangeCode.Equals("BO"))
                {
                    //read stock master from BSE
                    //i do not have the url to download that
                }
                if (reader != null)
                {
                    //read first line which is list of fields
                    reader.ReadLine();
                    sourceFile = new StringBuilder(reader.ReadToEnd());

                    if (reader != null)
                        reader.Close();
                    if (receiveStream != null)
                        receiveStream.Close();

                    sourceLines = sourceFile.ToString().Split('\n');
                    sourceFile.Clear();

                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    var transaction = sqlite_conn.BeginTransaction();
                    while (recCounter < sourceLines.Length)
                    {
                        record.Clear();
                        record.Append(sourceLines[recCounter++].Trim());
                        if (record.Length == 0)
                        {
                            continue;
                        }
                        fields = record.ToString().Split(',');

                        sqlite_cmd.CommandText = "REPLACE INTO STOCKMASTER(EXCHANGE, SYMBOL, COMP_NAME, SERIES, DATE_OF_LISTING, PAID_UP_VALUE, " +
                            "MARKET_LOT, ISIN_NUMBER, FACE_VALUE, LASTUPDT) " +
                            "VALUES (@EXCHANGE, @SYMBOL, @COMP_NAME, @SERIES, @DATE_OF_LISTING, @PAID_UP_VALUE, @MARKET_LOT, @ISIN_NUMBER, @FACE_VALUE, @LASTUPDT)";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@EXCHANGE", exchangeCode);
                        sqlite_cmd.Parameters.AddWithValue("@SYMBOL", fields[0]);
                        sqlite_cmd.Parameters.AddWithValue("@COMP_NAME", fields[1]);
                        sqlite_cmd.Parameters.AddWithValue("@SERIES", fields[2]);
                        sqlite_cmd.Parameters.AddWithValue("@DATE_OF_LISTING", System.Convert.ToDateTime(fields[3]).ToString("yyyy-MM-dd"));
                        sqlite_cmd.Parameters.AddWithValue("@PAID_UP_VALUE", fields[4]);
                        sqlite_cmd.Parameters.AddWithValue("@MARKET_LOT", fields[5]);
                        sqlite_cmd.Parameters.AddWithValue("@ISIN_NUMBER", fields[6]);
                        sqlite_cmd.Parameters.AddWithValue("@FACE_VALUE", fields[7]);
                        sqlite_cmd.Parameters.AddWithValue("@LASTUPDT", dateToday);
                        try
                        {
                            numOfRowsInserted += sqlite_cmd.ExecuteNonQuery();
                        }
                        catch (SQLiteException sqlException)
                        {
                            Console.WriteLine(sqlException.Message);
                            break;
                        }
                    }
                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
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
            }
            return numOfRowsInserted;
        }

        /// <summary>
        /// Method to parse the output JSON received by calling yahoo url to get quote using range = 1m and interval = 1m
        /// </summary>
        /// <param name="record">JSON string</param>
        /// <param name="symbol">Symbol for which we fetched the quote</param>
        /// <returns>DataTable or null</returns>
        public DataTable getQuoteTableFromJSON(string record, string symbol)
        {
            DataTable resultDataTable = null;
            DateTime myDate;
            double close;
            double high;
            double low;
            double open;
            int volume;
            double change;
            double changepercent;
            double prevclose;
            //double adjusetedClose = 0.00;
            //string formatedDate;
            var errors = new List<string>();
            try
            {
                Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(record, new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    DefaultValueHandling = DefaultValueHandling.Populate,
                    Error = delegate (object sender, Newtonsoft.Json.Serialization.ErrorEventArgs args)
                    {
                        errors.Add(args.ErrorContext.Error.Message);
                        args.ErrorContext.Handled = true;
                        //args.ErrorContext.Handled = false;
                    }
                    //Converters = { new IsoDateTimeConverter() }

                });

                Chart myChart = myDeserializedClass.chart;

                Result myResult = myChart.result[0];

                Meta myMeta = myResult.meta;

                Indicators myIndicators = myResult.indicators;

                //this will be typically only 1 row and quote will have list of close, high, low, open, volume
                Quote myQuote = myIndicators.quote[0];

                //this will be typically only 1 row and adjClose will have list of adjClose
                //Adjclose myAdjClose = null;
                //if (bIsDaily)
                //{
                //    myAdjClose = myIndicators.adjclose[0];
                //}

                if (myResult.timestamp != null)
                {
                    resultDataTable = new DataTable();

                    resultDataTable.Columns.Add("Symbol", typeof(string));
                    resultDataTable.Columns.Add("Open", typeof(decimal));
                    resultDataTable.Columns.Add("High", typeof(decimal));
                    resultDataTable.Columns.Add("Low", typeof(decimal));
                    resultDataTable.Columns.Add("Price", typeof(decimal));
                    resultDataTable.Columns.Add("Volume", typeof(int));
                    resultDataTable.Columns.Add("latestDay", typeof(DateTime));
                    resultDataTable.Columns.Add("previousClose", typeof(decimal));
                    resultDataTable.Columns.Add("change", typeof(decimal));
                    resultDataTable.Columns.Add("changePercent", typeof(decimal));

                    for (int i = 0; i < myResult.timestamp.Count; i++)
                    {
                        if ((myQuote.close[i] == null) && (myQuote.high[i] == null) && (myQuote.low[i] == null) && (myQuote.open[i] == null)
                            && (myQuote.volume[i] == null))
                        {
                            continue;
                        }

                        //myDate = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(myResult.timestamp[i]).ToLocalTime();
                        myDate = convertUnixEpochToLocalDateTime(myResult.timestamp[i], myMeta.timezone);

                        //myDate = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(myResult.timestamp[i]);
                        //string formatedDate = myDate.ToString("dd-MM-yyyy");
                        //formatedDate = myDate.ToString("yyyy-dd-MM");

                        //myDate = System.Convert.ToDateTime(myResult.timestamp[i]);

                        //if all are null do not enter this row

                        if (myQuote.close[i] == null)
                        {
                            close = 0.00;
                        }
                        else
                        {
                            //close = (double)myQuote.close[i];
                            close = System.Convert.ToDouble(string.Format("{0:0.00}", myQuote.close[i]));
                        }

                        if (myQuote.high[i] == null)
                        {
                            high = 0.00;
                        }
                        else
                        {
                            //high = (double)myQuote.high[i];
                            high = System.Convert.ToDouble(string.Format("{0:0.00}", myQuote.high[i]));
                        }

                        if (myQuote.low[i] == null)
                        {
                            low = 0.00;
                        }
                        else
                        {
                            //low = (double)myQuote.low[i];
                            low = System.Convert.ToDouble(string.Format("{0:0.00}", myQuote.low[i]));
                        }

                        if (myQuote.open[i] == null)
                        {
                            open = 0.00;
                        }
                        else
                        {
                            //open = (double)myQuote.open[i];
                            open = System.Convert.ToDouble(string.Format("{0:0.00}", myQuote.open[i]));
                        }
                        if (myQuote.volume[i] == null)
                        {
                            volume = 0;
                        }
                        else
                        {
                            volume = (int)myQuote.volume[i];
                        }
                        prevclose = System.Convert.ToDouble(string.Format("{0:0.00}", myMeta.chartPreviousClose));
                        change = close - prevclose;
                        changepercent = (change / prevclose) * 100;
                        change = System.Convert.ToDouble(string.Format("{0:0.00}", change));
                        changepercent = System.Convert.ToDouble(string.Format("{0:0.00}", changepercent));

                        resultDataTable.Rows.Add(new object[] {
                                                                    symbol,
                                                                    Math.Round(open, 4),
                                                                    Math.Round(high, 4),
                                                                    Math.Round(low, 4),
                                                                    Math.Round(close, 4),
                                                                    volume,
                                                                    myDate,
                                                                    Math.Round(prevclose, 4),
                                                                    Math.Round(change, 4),
                                                                    Math.Round(changepercent, 4)
                                                                    //adjusetedClose
                                                                });
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                if (resultDataTable != null)
                {
                    resultDataTable.Clear();
                    resultDataTable.Dispose();
                }
                resultDataTable = null;
            }
            return resultDataTable;
        }

        /// <summary>
        /// Method to fetch quote for given symbol + exchange combinations using range = 1m inerval = 1m parameters
        /// It will call yahoo quote url and get JSON as output
        /// It will parse the JSON and put the data in DataTable
        /// </summary>
        /// <param name="symbol">Symbol for which to get quote</param>
        /// <param name="exchange">Exchange to which this symbol belongs - BSE or NSE</param>
        /// <returns></returns>
        public DataTable GetQuote(string symbol)
        {
            DataTable resultDataTable = null;
            try
            {
                string webservice_url = "";
                WebResponse wr;
                Stream receiveStream = null;
                StreamReader reader = null;
                DataRow r;

                //https://query1.finance.yahoo.com/v7/finance/chart/HDFC.BO?range=1m&interval=1m&indicators=quote&timestamp=true
                webservice_url = string.Format(StockManager.urlGlobalQuote, symbol);

                Uri url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);

                resultDataTable = getQuoteTableFromJSON(reader.ReadToEnd(), symbol);
                reader.Close();
                if (receiveStream != null)
                    receiveStream.Close();
            }
            catch (Exception ex)
            {
                if (resultDataTable != null)
                {
                    resultDataTable.Clear();
                    resultDataTable.Dispose();
                }
                resultDataTable = null;
            }
            return resultDataTable;
        }

        #endregion

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

        public DataTable SearchStock(string symbol, string exchangeCode = "NS")
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
                sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME FROM STOCKMASTER WHERE SYMBOL = '" + symbol + "' AND EXCHANGE = '" + exchangeCode + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getStockMaster: " + exSQL.Message);
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

        public DataTable getStockMaster(string exchangeCode = "NS")
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
                sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME FROM STOCKMASTER WHERE EXCHANGE = '" + exchangeCode + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getStockMaster: " + exSQL.Message);
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

        public string GetRange(string time_interval, string outputsize)
        {
            StringBuilder range = new StringBuilder();
            if (time_interval.Equals("1d"))
            {
                if (outputsize.Equals("compact"))
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
                if (outputsize.Equals("compact"))
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
                if (outputsize.Equals("compact"))
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
                if (outputsize.Equals("compact"))
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
        public StringBuilder FetchStockDataOnline(string scriptname, string range, string interval, string indicators = "quote", bool includeTimestamps = true)
        {
            StringBuilder returnData = null;
            string webservice_url = "";
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            try
            {
                webservice_url = string.Format(StockManager.urlGetStockData, scriptname, range, interval, indicators, includeTimestamps);

                Uri url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                returnData = new StringBuilder(reader.ReadToEnd());
            }
            catch (Exception ex)
            {

            }
            return returnData;
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

        public bool InsertStockData(StringBuilder record, string symbol, string exchangename = "NSE", string type = "EQ", string time_interval = "1d", SQLiteCommand sqlite_cmd = null)
        {
            bool breturn = true;
            DateTime myDate;
            double close;
            double high;
            double low;
            double open;
            long volume;
            double adjusetedClose = 0.00;

            SQLiteConnection sqlite_conn = null;
            SQLiteTransaction transaction = null;

            var errors = new List<string>();

            try
            {
                Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(record.ToString(), new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore,
                    DefaultValueHandling = DefaultValueHandling.Populate,
                    Error = delegate (object sender, Newtonsoft.Json.Serialization.ErrorEventArgs args)
                    {
                        errors.Add(args.ErrorContext.Error.Message);
                        args.ErrorContext.Handled = true;
                        //args.ErrorContext.Handled = false;
                    }
                    //Converters = { new IsoDateTimeConverter() }

                });

                Chart myChart = myDeserializedClass.chart;

                Result myResult = myChart.result[0];

                Meta myMeta = myResult.meta;

                Indicators myIndicators = myResult.indicators;

                //this will be typically only 1 row and quote will have list of close, high, low, open, volume
                Quote myQuote = myIndicators.quote[0];

                //this will be typically only 1 row and adjClose will have list of adjClose. It is NOT available when we call for intra - 1m, 3m, 5m, 30m, 60m
                Adjclose myAdjClose = null;
                if (time_interval.Contains("m") == false)
                {
                    myAdjClose = myIndicators.adjclose[0];
                }
                if (myResult.timestamp != null)
                {
                    if (sqlite_cmd == null)
                    {
                        sqlite_conn = CreateConnection();
                        sqlite_cmd = sqlite_conn.CreateCommand();
                        transaction = sqlite_conn.BeginTransaction();
                    }
                    try
                    {
                        for (int i = 0; i < myResult.timestamp.Count; i++)
                        {
                            if ((myQuote.close[i] == null) && (myQuote.high[i] == null) && (myQuote.low[i] == null) && (myQuote.open[i] == null)
                                && (myQuote.volume[i] == null))
                            {
                                continue;
                            }
                            myDate = convertUnixEpochToLocalDateTime(myResult.timestamp[i], myMeta.timezone);

                            if (myQuote.open[i] == null)
                            {
                                open = 0.00;
                            }
                            else
                            {
                                //open = (double)myQuote.open[i];
                                open = System.Convert.ToDouble(string.Format("{0:0.0000}", myQuote.open[i]));
                            }

                            if (myQuote.high[i] == null)
                            {
                                high = 0.00;
                            }
                            else
                            {
                                //high = (double)myQuote.high[i];
                                high = System.Convert.ToDouble(string.Format("{0:0.0000}", myQuote.high[i]));
                            }

                            if (myQuote.low[i] == null)
                            {
                                low = 0.00;
                            }
                            else
                            {
                                //low = (double)myQuote.low[i];
                                low = System.Convert.ToDouble(string.Format("{0:0.0000}", myQuote.low[i]));
                            }

                            if (myQuote.close[i] == null)
                            {
                                close = 0.00;
                            }
                            else
                            {
                                //close = (double)myQuote.close[i];
                                close = System.Convert.ToDouble(string.Format("{0:0.00}", myQuote.close[i]));
                            }

                            if (myQuote.volume[i] == null)
                            {
                                volume = 0;
                            }
                            else
                            {
                                volume = (long)myQuote.volume[i];
                            }

                            if (myAdjClose == null)
                            {
                                adjusetedClose = 0.00;
                            }
                            else
                            {
                                adjusetedClose = System.Convert.ToDouble(string.Format("{0:0.00}", myAdjClose.adjclose[i]));
                            }

                            sqlite_cmd.CommandText = "REPLACE INTO  STOCKDATA(SYMBOL, EXCHANGENAME, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, TIMESTAMP) " +
                           "VALUES (@SYMBOL, @EXCHANGENAME, @TYPE, @DATA_GRANULARITY, @OPEN, @HIGH, @LOW, @CLOSE, @ADJ_CLOSE, @VOLUME, @TIMESTAMP)";

                            sqlite_cmd.Prepare();
                            sqlite_cmd.Parameters.AddWithValue("@SYMBOL", symbol);
                            sqlite_cmd.Parameters.AddWithValue("@EXCHANGENAME", exchangename);
                            sqlite_cmd.Parameters.AddWithValue("@TYPE", type);
                            sqlite_cmd.Parameters.AddWithValue("@DATA_GRANULARITY", time_interval);
                            sqlite_cmd.Parameters.AddWithValue("@OPEN", open);
                            sqlite_cmd.Parameters.AddWithValue("@HIGH", high);
                            sqlite_cmd.Parameters.AddWithValue("@LOW", low);
                            sqlite_cmd.Parameters.AddWithValue("@CLOSE", close);
                            sqlite_cmd.Parameters.AddWithValue("@ADJ_CLOSE", adjusetedClose);
                            sqlite_cmd.Parameters.AddWithValue("@VOLUME", volume);
                            sqlite_cmd.Parameters.AddWithValue("@TIMESTAMP", myDate);

                            sqlite_cmd.ExecuteNonQuery();
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("InsertStockData: [" + symbol + "] " + exSQL.Message);
                        breturn = false;
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
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("InsertStockData: [" + symbol + "] " + ex.Message);
                breturn = false;
            }
            return breturn;
        }
        /// <summary>
        /// First check based on the parameters if equivalant data exist in DB. If yes, do not do anything
        /// If data does not exist or partially exist, then fetch appropriate data from yahoo url and save it to db
        /// validRanges": [ "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max" ]
        /// Please refer to GetRange method to understand how this is used to find range
        /// </summary>
        /// <param name="scriptname">Stock market Symbol of the company</param>
        /// <param name="exchangeName"></param>
        /// <param name="outputsize"></param>
        /// <param name="time_interval">[1m, 5m, 15m, 30m, 60m, "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max" ]</param>
        /// <returns>num of recrods inserted or if no need to insert then 0 or -1 in case of error</returns>
        public bool FetchAndSaveDailyStockData(string scriptname, string exchangeName = "NSE", string equitytype = "EQ", string outputsize = "compact", string time_interval = "1d")
        {
            bool breturn = true;
            string convertedScriptName;
            DateTime datetimeMaxTimestamp = time_interval.Contains("m") ? DateTime.Now : DateTime.Today;
            DateTime datetimeToday = time_interval.Contains("m") ? DateTime.Now : DateTime.Today;
            string range;
            int compare = -1;
            //this is daily for the given script name first find from database if given time interval data exists. If it does not then find the last
            //timestamp and get data from that point till now
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                //we need to do this as yahoo idenitifies indian stocks by appending .exchangecode to stock symbol
                if (exchangeName.Equals("BSE"))
                {
                    convertedScriptName = scriptname + ".BO";
                }
                else if (exchangeName.Equals("NSE"))
                {
                    convertedScriptName = scriptname + ".NS";
                }
                else
                {
                    convertedScriptName = scriptname;
                }

                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = "SELECT max(timestamp) FROM STOCKDATA WHERE SYMBOL = '" + scriptname + "' AND DATA_GRANULARITY = '" + time_interval + "'";
                try
                {
                    var tsObject = sqlite_cmd.ExecuteScalar();

                    if (tsObject.ToString().Equals("") == false)
                    {
                        //that means there is data
                        datetimeMaxTimestamp = System.Convert.ToDateTime(tsObject);

                        //we want to check if the lasttimestamp in DB is same as today or not
                        //compare returns < 0 if dt1 earlier than dt2, =0 if dt1 = dt2, > 0 dt1 later than dt2

                        compare = DateTime.Compare(datetimeMaxTimestamp, datetimeToday);
                    }

                    if (compare < 0)
                    {
                        TimeSpan diffSpan = datetimeToday - datetimeMaxTimestamp;
                        //this gives us diff in days:hour:min:sec


                        range = GetRange(time_interval, outputsize);
                        //we need to fetch data starting from lasttimestamp. We need to send converted script name with either .NS or .BO
                        StringBuilder sbStockData = FetchStockDataOnline(convertedScriptName, range, time_interval, indicators, true);
                        breturn = InsertStockData(sbStockData, scriptname, exchangeName, equitytype, time_interval, sqlite_cmd);
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getStockMaster: " + exSQL.Message);
                }
                finally
                {
                    transaction.Commit();
                    transaction.Dispose();
                    transaction = null;
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
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseTable: " + ex.Message);
            }

            return breturn;
        }

        public int getPortfolioCount(string userId, SQLiteCommand sqlite_cmd = null)
        {
            int portfoliocount = 0;
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
                sqlite_cmd.CommandText = "SELECT count(ROWID) as NUMOFPORTFOLIO FROM STOCKPORTFOLIO_MASTER WHERE USERID = '" + userId + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        portfoliocount = Int32.Parse(sqlite_datareader["NUMOFPORTFOLIO"].ToString());
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getPortfoliId: [" + userId + "]" + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getPortfoliId: [" + userId + "]" + ex.Message);
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
            return portfoliocount;
        }

        /// <summary>
        /// THis method will fetch ROWID from STOCKPORTFOLIO_MASTER table for the provided userid + portfolioname combnation
        /// </summary>
        /// <param name="portfolioName">name of the portfolio</param>
        /// <param name="userId">emailid of current user</param>
        /// <param name="sqlite_cmd">command param if called under transaction block from caller</param>
        /// <returns>-1 if no match found else ROWID of the given portfolioname+userid combination</returns>
        public long getPortfolioId(string userId, string portfolioName, SQLiteCommand sqlite_cmd = null)
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
                sqlite_cmd.CommandText = "SELECT ROWID FROM STOCKPORTFOLIO_MASTER WHERE USERID = '" + userId + "' AND PORTFOLIO_NAME = '" + portfolioName + "'";
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


        /// <summary>
        /// THis methid will create a row in STOCKPORTFOLIO_MASTER table and return the rowid for the portfolio that was inserted
        /// Caller should refer to this portfolio id, when they want to perform any operations using this portfolio
        /// </summary>
        /// <param name="userid">emailid of the current user</param>
        /// <param name="portfolioname">name of the portfolio</param>
        /// <returns>ROWID of the current row</returns>
        public long createNewPortfolio(string userid, string portfolioname)
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
                    sqlite_cmd.CommandText = "INSERT OR IGNORE INTO   STOCKPORTFOLIO_MASTER(USERID, PORTFOLIO_NAME) VALUES (@USERID, @NAME)";
                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@USERID", userid);
                    sqlite_cmd.Parameters.AddWithValue("@NAME", portfolioname);
                    if (sqlite_cmd.ExecuteNonQuery() > 0)
                    {
                        portfolio_id = getPortfolioId(userid, portfolioname, sqlite_cmd);
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

        /// <summary>
        /// Method to add transaction for the provided portfolio masterrowid and for the stock provided by stockmasterrowid
        /// </summary>
        /// <param name="masterrowid">ROWID of the portfoliname from STOCKPORTFOLIO_MASTER</param>
        /// <param name="stockmasterrowid">ROWID of the stock from STOCKMASTER</param>
        /// <param name="symbol"></param>
        /// <param name="price"></param>
        /// <param name="date"></param>
        /// <param name="qty"></param>
        /// <param name="commission"></param>
        /// <param name="cost"></param>
        /// <param name="companyname"></param>
        /// <param name="exch"></param>
        /// <param name="type"></param>
        /// <param name="exchDisp"></param>
        /// <param name="typeDisp"></param>
        /// <returns>true if row inserted successfully else false</returns>
        public bool insertNode(string masterrowid, string stockmasterrowid, string symbol, string price, string date, string qty, string commission,
                                string cost)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteTransaction transaction = null;
            SQLiteCommand sqlite_cmd = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                transaction = sqlite_conn.BeginTransaction();
                try
                {

                    sqlite_cmd.CommandText = "INSERT OR IGNORE INTO  STOCKPORTFOLIO(MASTER_ROWID, STOCKMASTER_ROWID, PURCHASE_DATE, PURCHASE_PRICE, " +
                                             "PURCHASE_QTY, COMMISSION_TAXES, INVESTMENT_COST) " +
                                             "VALUES (@MASTER_ROWID, @STOCKMASTER_ROWID, @PURCHASE_DATE, @PURCHASE_PRICE, " +
                                             "@PURCHASE_QTY, @COMMISSION_TAXES, @INVESTMENT_COST)";
                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@MASTER_ROWID", masterrowid);
                    sqlite_cmd.Parameters.AddWithValue("@STOCKMASTER_ROWID", stockmasterrowid);
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(date).ToString("yyyy-MM-dd"));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_PRICE", string.Format("{0:0:0000}", price));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_QTY", string.Format("{0:0.0000}", qty));
                    sqlite_cmd.Parameters.AddWithValue("@COMMISSION_TAXES", string.Format("{0:0.0000}", commission));
                    sqlite_cmd.Parameters.AddWithValue("@INVESTMENT_COST", string.Format("{0:0.0000}", cost));
                    if (sqlite_cmd.ExecuteNonQuery() > 0)
                    {
                        breturn = true;
                    }

                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("addNewTransaction: [" + masterrowid + "," + stockmasterrowid + "," + date + "," + qty + price + "] " + exSQL.Message);
                }
                transaction.Commit();
            }
            catch (Exception ex)
            {
                Console.WriteLine("addNewTransaction: [" + masterrowid + "," + stockmasterrowid + "," + date + "," + qty + price + "] " + ex.Message);
            }
            finally
            {
                if (sqlite_conn != null)
                {
                    if (transaction != null)
                    {
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

        /// <summary>
        /// Method to update an existing transaction. The transaction is identified by the ROWID of the STOCKPORTFOLIO
        /// Only price, date, quantity, commission and investment cost will be updated
        /// </summary>
        /// <param name="stockportfolio_rowid">ROWID of the current transaction from STOCKPORTFOLIO</param>
        /// <param name="newprice"></param>
        /// <param name="newdate"></param>
        /// <param name="newqty"></param>
        /// <param name="newcommission"></param>
        /// <param name="newcost"></param>
        /// <returns>trus if transaction updated successfully else false</returns>
        public bool updateNode(string stockportfolio_rowid, string newprice, string newdate, string newqty, string newcommission, string newcost)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();


                sqlite_cmd.CommandText = "UPDATE STOCKPORTFOLIO SET PURCHASE_DATE = @PURCHASE_DATE, PURCHASE_PRICE = @PURCHASE_PRICE, " +
                                        "PURCHASE_QTY = @PURCHASE_QTY, COMMISSION_TAXES = @COMMISSION_TAXES, INVESTMENT_COST = @INVESTMENT_COST " +
                                        "WHERE ROWID = " + stockportfolio_rowid;


                sqlite_cmd.Prepare();
                sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(newdate).ToString("yyyy-MM-dd"));
                sqlite_cmd.Parameters.AddWithValue("@PURCHASE_PRICE", string.Format("{0:0:0000}", newprice));
                sqlite_cmd.Parameters.AddWithValue("@PURCHASE_QTY", string.Format("{0:0.0000}", newqty));
                sqlite_cmd.Parameters.AddWithValue("@COMMISSION_TAXES", string.Format("{0:0.0000}", newcommission));
                sqlite_cmd.Parameters.AddWithValue("@INVESTMENT_COST", string.Format("{0:0.0000}", newcost));

                try
                {
                    if (sqlite_cmd.ExecuteNonQuery() > 0)
                    {
                        breturn = true;
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("updateTransaction: [" + stockportfolio_rowid + "] " + exSQL.Message);
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

        /// <summary>
        /// THis method deletes a transaction row from given stockportfolio_rowid
        /// </summary>
        /// <param name="stockportfolio_rowid">ROWID of the STOCKPORTFOLIO</param>
        /// <returns>true if row deleted successfully else false</returns>
        public bool deleteNode(string stockportfolio_rowid)
        {
            bool breturn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();

                {
                    sqlite_cmd.CommandText = "DELETE FROM STOCKPORTFOLIO WHERE ROWID = " + stockportfolio_rowid;

                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            breturn = true;
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("deletePortfolioRow: [" + stockportfolio_rowid + "] " + exSQL.Message);
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

        /// <summary>
        /// Method returns all rows from STOCKPORTFOLIO_MASTER for the userid supplied
        /// </summary>
        /// <param name="userid">userid for whom to get portfolio list</param>
        /// <returns>DataTable containing rows with ROWID, PORTFOLIO_NAME from STOCKPORTFOLIO_MASTER or null</returns>
        public DataTable getPortfolioMaster(string userid)
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
                sqlite_cmd.CommandText = "SELECT ROWID, PORTFOLIO_NAME FROM STOCKPORTFOLIO_MASTER WHERE USERID = '" + userid + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getStockMaster: " + exSQL.Message);
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
        /// Method called to get portfolio table and individual valuation for each transaction done in respective SYMBOL
        /// </summary>
        /// <param name="portfolioFileName"></param>
        /// <param name="stockportfolioMasterRowId"></param>
        /// <param name="bCurrent"></param>
        /// <returns></returns>
        public DataTable getStockPortfolioTable(string stockportfolioMasterRowId)
        {
            DataTable resultDataTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            string statement = "SELECT STOCKPORTFOLIO.ROWID as ID, STOCKPORTFOLIO.MASTER_ROWID as MASTERID, STOCKPORTFOLIO.STOCKMASTER_ROWID as STOCKID, " +
                "STOCKMASTER.EXCHANGE, (STOCKMASTER.SYMBOL || '.' || STOCKMASTER.EXCHANGE) as SYMBOL, STOCKMASTER.COMP_NAME, STOCKMASTER.SERIES, " +
                "strftime('%d-%m-%Y', STOCKPORTFOLIO.PURCHASE_DATE) AS PURCHASE_DATE, STOCKPORTFOLIO.PURCHASE_PRICE, STOCKPORTFOLIO.PURCHASE_QTY, " +
                "STOCKPORTFOLIO.COMMISSION_TAXES, STOCKPORTFOLIO.INVESTMENT_COST FROM STOCKMASTER " +
                "INNER JOIN STOCKPORTFOLIO ON STOCKPORTFOLIO.STOCKMASTER_ROWID = STOCKMASTER.ROWID " +
                "WHERE STOCKPORTFOLIO.MASTER_ROWID = " + stockportfolioMasterRowId + " " +
                "ORDER BY STOCKPORTFOLIO.STOCKMASTER_ROWID ASC, STOCKPORTFOLIO.PURCHASE_DATE DESC";

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
                    resultDataTable.Columns.Add("MASTERID", typeof(long)); //FundHouse
                    resultDataTable.Columns.Add("STOCKID", typeof(long)); //FundHouse
                    resultDataTable.Columns.Add("EXCHANGE", typeof(string)); //FundHouse
                    resultDataTable.Columns.Add("SYMBOL", typeof(string)); //FundHouse
                    resultDataTable.Columns.Add("COMP_NAME", typeof(string)); //FundName
                    resultDataTable.Columns.Add("SERIES", typeof(string)); //SCHEME_CODE
                    resultDataTable.Columns.Add("PURCHASE_DATE", typeof(DateTime)); //PurchaseDate
                    resultDataTable.Columns.Add("PURCHASE_PRICE", typeof(decimal)); //PurchaseNAV
                    resultDataTable.Columns.Add("PURCHASE_QTY", typeof(decimal)); //PurchaseUnits
                    resultDataTable.Columns.Add("COMMISSION_TAXES", typeof(decimal)); //ValueAtCost
                    resultDataTable.Columns.Add("INVESTMENT_COST", typeof(decimal)); //ValueAtCost

                    resultDataTable.Columns.Add("CURRENTDATE", typeof(DateTime)); //PurchaseDate
                    resultDataTable.Columns.Add("CURRENTPRICE", typeof(decimal));
                    resultDataTable.Columns.Add("CURRENTVALUE", typeof(decimal));
                    resultDataTable.Columns.Add("YearsInvested", typeof(decimal));
                    resultDataTable.Columns.Add("ARR", typeof(decimal));
                    resultDataTable.RowChanged += new DataRowChangeEventHandler(handlerStockPortfolioTableRowChanged);

                    resultDataTable.Load(sqlite_datareader);
                    resultDataTable.RowChanged -= new DataRowChangeEventHandler(handlerStockPortfolioTableRowChanged);
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

        /// <summary>
        /// method that gets called when a row is added to 'sender' data table
        /// We will check the latest quote and then calculate the current valuation
        /// We will do this only when a transaction with different symbol is available in 'e'
        /// If the previous row has the same Symbol then we will just use the same price
        /// </summary>
        /// <param name="sender">Source DataTable</param>
        /// <param name="e">Current Row</param>
        private void handlerStockPortfolioTableRowChanged(object sender, DataRowChangeEventArgs e)
        {
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerStockPortfolioTableRowChanged);

            double currentPrice = 0.00, purchasePrice = 0.00, purchaseQty = 0.00, investmentCost = 0.00, currentValue = 0.00, yearsInvested = 0.00, arr = 0.00;
            DateTime currentDate, purchaseDate;
            currentDate = DateTime.MinValue;

            //first check if this is transaction for new symbol or for same symbol. Depending that currentPrice and CurrentDate will be assigned
            DataTable sourceTable = (DataTable)sender;
            int rowIndex = sourceTable.Rows.Count;
            rowIndex--; //this is the current row represented by e
            if (sourceTable.Rows.Count > 1)
            {
                rowIndex--; //this is the row prior to e
            }

            //if this is the first row or if the current row symbol is different than previous row symbol - fetch quote from market
            if ((rowIndex == 0) || (e.Row["SYMBOL"].ToString().Equals(sourceTable.Rows[rowIndex]["SYMBOL"].ToString()) == false))
            {
                DataTable quoteTable = GetQuote(e.Row["SYMBOL"].ToString());
                if (quoteTable != null)
                {
                    currentPrice = System.Convert.ToDouble(string.Format("{0:0.00}", quoteTable.Rows[0]["price"].ToString()));
                    currentDate = System.Convert.ToDateTime(quoteTable.Rows[0]["latestDay"].ToString());
                }

            }
            else //the rows have same symbol use the current price from previous row
            {
                currentPrice = System.Convert.ToDouble(string.Format("{0:0.00}", sourceTable.Rows[rowIndex]["CURRENTPRICE"].ToString()));
                currentDate = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["CURRENTDATE"].ToString());
            }

            currentValue = Math.Round(currentPrice * System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["PURCHASE_QTY"])), 4);
            purchaseDate = System.Convert.ToDateTime(e.Row["PURCHASE_DATE"].ToString());

            try
            {
                yearsInvested = Math.Round(((currentDate - purchaseDate).TotalDays) / 365.25, 4);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                yearsInvested = Math.Round(0.00, 4);
            }
            investmentCost = System.Convert.ToDouble(string.Format("{0:0.0000}", e.Row["INVESTMENT_COST"]));

            try
            {
                arr = Math.Round(0.00, 4);
                if (yearsInvested > 0)
                {
                    arr = Math.Round(Math.Pow((currentValue / investmentCost), (1 / yearsInvested)) - 1, 4);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                arr = Math.Round(0.00, 4);
            }
            //e.Row["PurchaseDate"] = purchasePrice.ToString("yyyy-MM-dd");
            //e.Row["PurchaseNAV"] = string.Format("{0:0.0000}", e.Row["PurchaseNAV"]);
            //e.Row["PurchaseUnits"] = string.Format("{0:0.0000}", e.Row["PurchaseUnits"]);
            //e.Row["ValueAtCost"] = string.Format("{0:0.0000}", valueAtCost);

            e.Row["CURRENTPRICE"] = string.Format("{0:0.0000}", currentPrice);
            e.Row["CURRENTDATE"] = currentDate.ToString("yyyy-MM-dd HH:mm:ss");
            e.Row["CURRENTVALUE"] = string.Format("{0:0.0000}", currentValue);
            e.Row["YearsInvested"] = string.Format("{0:0.0000}", yearsInvested);
            e.Row["ARR"] = string.Format("{0:0.0000}", arr);

            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerStockPortfolioTableRowChanged);
        }

        /// <summary>
        /// Method to delete selected portfolio transaction from STOCKPORTFOLIO
        /// </summary>
        /// <param name="portfolioRowId">rowid of the selected transactions</param>
        /// <returns>true if transaction deleted successfully else false</returns>
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

                sqlite_cmd.CommandText = "DELETE FROM STOCKPORTFOLIO WHERE ROWID = " + portfolioRowId;

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

        public bool updateTransaction(string stockportfolioRowId, string purchasePrice, string purchaseDate, string purchaseQty, string commissionPaid, string totalCost)
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

                {
                    sqlite_cmd.CommandText = "UPDATE STOCKPORTFOLIO SET PURCHASE_DATE = @PURCHASE_DATE, PURCHASE_PRICE = @PURCHASE_PRICE, PURCHASE_QTY = @PURCHASE_QTY, " +
                                            "COMMISSION_TAXES = @COMMISSION_TAXES, INVESTMENT_COST = @INVESTMENT_COST " +
                                            "WHERE ROWID = " + stockportfolioRowId;

                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(purchaseDate).ToString("yyyy-MM-dd"));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_PRICE", string.Format("{0:0:0000}", purchasePrice));
                    sqlite_cmd.Parameters.AddWithValue("@PURCHASE_QTY", string.Format("{0:0.0000}", purchaseQty));
                    sqlite_cmd.Parameters.AddWithValue("@COMMISSION_TAXES", string.Format("{0:0.0000}", commissionPaid));
                    sqlite_cmd.Parameters.AddWithValue("@INVESTMENT_COST", string.Format("{0:0.0000}", totalCost));

                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            breturn = true;
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("updateTransaction: [" + stockportfolioRowId + "] " + exSQL.Message);
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

    }
}
