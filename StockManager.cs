using ClosedXML.Excel;
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
using System.Xml;

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

        //to get specific historic price
        //below is 10-02-2022 to 11-02-2022
        //https://finance.yahoo.com/quote/LT.NS/history?period1=1644451200&period2=1644537600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true

        //below is 17-02-2022 to 18-02-2022
        //https://finance.yahoo.com/quote/LT.NS/history?period1=1645056000&period2=1645142400&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
        //the following gives json
        //https://query1.finance.yahoo.com/v8/finance/chart/LT.NS?period1=1644451200&period2=1644537600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true

        public static string urlGetHistoryQuote = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?period1={1}&period2={2}&interval={3}&filter=history&frequency={4}&includeAdjustedClose={5}";

        //https://finance.yahoo.com/lookup/index?s=all
        //https://finance.yahoo.com/lookup/all?s=proctor
        //https://finance.yahoo.com/lookup/all?s=larsen
        public static string urlSearch = "https://finance.yahoo.com/lookup/{0}?s={1}";
        #region online menthods

        public Root getIndexIntraDayAlternate(string scriptName, string time_interval = "5min", string outputsize = "full")
        {
            Root myDeserializedClass = null;
            try
            {
                string webservice_url = "";
                WebResponse wr;
                Stream receiveStream = null;
                StreamReader reader = null;
                //string convertedScriptName;
                string range, interval;
                var errors = new List<string>();

                if (time_interval == "60min")
                {
                    interval = "60m";
                    if (outputsize.Equals("compact"))
                    {
                        range = "1d";
                    }
                    else
                    {
                        range = "2y";
                    }

                }
                else if (time_interval == "1min")
                {
                    interval = "1m";
                    if (outputsize.Equals("compact"))
                    {
                        range = "1d";
                    }
                    else
                    {
                        range = "7d";
                    }

                }
                else if (time_interval == "15min")
                {
                    interval = "15m";
                    if (outputsize.Equals("compact"))
                    {
                        range = "1d";
                    }
                    else
                    {
                        range = "60d";
                    }

                }
                else if (time_interval == "30min")
                {
                    interval = "30m";
                    if (outputsize.Equals("compact"))
                    {
                        range = "1d";
                    }
                    else
                    {
                        range = "60d";
                    }

                }
                else //if(time_interval == "60min")
                {
                    interval = "5m";
                    if (outputsize.Equals("compact"))
                    {
                        range = "1d";
                    }
                    else
                    {
                        range = "60d";
                    }
                }

                webservice_url = string.Format(urlGetStockData, scriptName, range, interval, indicators, includeTimestamps);

                Uri url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);

                myDeserializedClass = JsonConvert.DeserializeObject<Root>(reader.ReadToEnd(), new JsonSerializerSettings
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

                //Chart myChart = myDeserializedClass.chart;

                //Result myResult = myChart.result[0];

                //Meta myMeta = myResult.meta;

                //Indicators myIndicators = myResult.indicators;

                ////this will be typically only 1 row and quote will have list of close, high, low, open, volume
                //Quote myQuote = myIndicators.quote[0];

                ////this will be typically only 1 row and adjClose will have list of adjClose
                //Adjclose myAdjClose = null;
                //if (bIsDaily)
                //{
                //    myAdjClose = myIndicators.adjclose[0];
                //}

                reader.Close();
                if (receiveStream != null)
                    receiveStream.Close();
            }
            catch (Exception ex)
            {
                myDeserializedClass = null;
                Console.WriteLine(ex.Message);
            }
            return myDeserializedClass;
        }

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

                        InsertNewStock(fields[0] + "." + "NS", "NSI", fields[1], "Stocks", System.Convert.ToDateTime(fields[3]).ToString("yyyy-MM-dd"), fields[4],
                            fields[5], fields[6], fields[7], dateToday, sqlite_cmd: sqlite_cmd);

                        ////sqlite_cmd.CommandText = "REPLACE INTO STOCKMASTER(EXCHANGE, SYMBOL, COMP_NAME, SERIES, DATE_OF_LISTING, PAID_UP_VALUE, " +
                        ////    "MARKET_LOT, ISIN_NUMBER, FACE_VALUE, LASTUPDT) " +
                        ////    "VALUES (@EXCHANGE, @SYMBOL, @COMP_NAME, @SERIES, @DATE_OF_LISTING, @PAID_UP_VALUE, @MARKET_LOT, @ISIN_NUMBER, @FACE_VALUE, @LASTUPDT)";

                        ////Adding IGNORE INTO skips the insert if there are any conflict related to primary key unique or other constraints without raising errors
                        //sqlite_cmd.CommandText = "INSERT  OR IGNORE INTO STOCKMASTER(EXCHANGE, SYMBOL, COMP_NAME, SERIES, DATE_OF_LISTING, PAID_UP_VALUE, " +
                        //    "MARKET_LOT, ISIN_NUMBER, FACE_VALUE, LASTUPDT) " +
                        //    "VALUES (@EXCHANGE, @SYMBOL, @COMP_NAME, @SERIES, @DATE_OF_LISTING, @PAID_UP_VALUE, @MARKET_LOT, @ISIN_NUMBER, @FACE_VALUE, @LASTUPDT)";

                        ////You can use INSERT INTO table(columns) values(..,..) ON CONFLICT DO NOTHING";

                        //sqlite_cmd.Prepare();
                        //sqlite_cmd.Parameters.AddWithValue("@EXCHANGE", exchangeCode);
                        //sqlite_cmd.Parameters.AddWithValue("@SYMBOL", fields[0]);
                        //sqlite_cmd.Parameters.AddWithValue("@COMP_NAME", fields[1]);
                        //sqlite_cmd.Parameters.AddWithValue("@SERIES", fields[2]);
                        //sqlite_cmd.Parameters.AddWithValue("@DATE_OF_LISTING", System.Convert.ToDateTime(fields[3]).ToString("yyyy-MM-dd"));
                        //sqlite_cmd.Parameters.AddWithValue("@PAID_UP_VALUE", fields[4]);
                        //sqlite_cmd.Parameters.AddWithValue("@MARKET_LOT", fields[5]);
                        //sqlite_cmd.Parameters.AddWithValue("@ISIN_NUMBER", fields[6]);
                        //sqlite_cmd.Parameters.AddWithValue("@FACE_VALUE", fields[7]);
                        //sqlite_cmd.Parameters.AddWithValue("@LASTUPDT", dateToday);
                        //try
                        //{
                        //    numOfRowsInserted += sqlite_cmd.ExecuteNonQuery();
                        //}
                        //catch (SQLiteException sqlException)
                        //{
                        //    Console.WriteLine(sqlException.Message);
                        //    break;
                        //}
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

            if (record.ToUpper().Contains("NOT FOUND"))
            {
                return null;
            }
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

        public bool InsertStockFromJSON(string record, string symbol, SQLiteCommand sqlite_cmd = null)
        {
            bool bReturn = true;

            if (record.ToUpper().Contains("NOT FOUND"))
            {
                return false;
            }
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
                if (myMeta != null)
                {
                    bReturn = InsertNewStock(myMeta.symbol, myMeta.exchangeName, myMeta.symbol, myMeta.instrumentType, sqlite_cmd: sqlite_cmd);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                bReturn = false;
            }
            return bReturn;
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
                //DataRow r;

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

        //https://query1.finance.yahoo.com/v8/finance/chart/LT.NS?period1=1644451200&period2=1644537600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
        //public static string urlGetHistoryQuote = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?period1={1}&period2={2}&interval={3}&filter=history&frequency={4}&includeAdjustedClose={5}";
        public DataTable GetHistoryQuote(string symbol, string periodDt1, string periodDt2, string interval = "1d", string frequency = "1d", string adjclose = "true")
        {
            DataTable resultDataTable = null;
            try
            {
                string webservice_url = "";
                WebResponse wr;
                Stream receiveStream = null;
                StreamReader reader = null;
                //DataRow r;

                //https://query1.finance.yahoo.com/v8/finance/chart/LT.NS?period1=1644451200&period2=1644537600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
                //public static string urlGetHistoryQuote = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?period1={1}&period2={2}&interval={3}&filter=history&frequency={4}&includeAdjustedClose={5}";

                //we need to convert the date first
                string period1 = convertDateTimeToUnixEpoch(System.Convert.ToDateTime(periodDt1)).ToString();
                string period2 = convertDateTimeToUnixEpoch(System.Convert.ToDateTime(periodDt2)).ToString();

                webservice_url = string.Format(StockManager.urlGetHistoryQuote, symbol, period1, period2, interval, frequency, adjclose);

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
        /// Method will use https://finance.yahoo.com/lookup/all?s=larsen to search input search string
        /// The output returned is an XML from which we are interested in
        /// <span>All (11)</span> if the search returns valid symbol list it will have number > 0 in () else it will have (0)
        /// <section>
        ///     <ul>
        ///         <li>
        ///             <a>
        ///                 <span> All (11)</span>
        ///             </a>
        ///         </li>
        ///     </ul>
        /// </section>
        /// THe data is in 
        /// <div> 
        ///     <div>
        ///         <div>
        ///             <table>
        ///                 <tbody>
        ///                     <tr>
        ///                         <td class="data-col0 Ta(start) Pstart(6px) Pend(15px)">
        /// symbol =                    <a href = "/quote/LT.NS?p=LT.NS" title="LARSEN &amp; TOUBRO" data-symbol="LT.NS" class="Fw(b)">LT.NS</a>
        ///                         </td>
        /// comp_name =             <td class="data-col1 Ta(start) Pstart(10px) Miw(80px)">LARSEN &amp; TOUBRO</td>
        /// last_price =            <td class="data-col2 Ta(end) Pstart(20px) Pend(15px)">1,925.30</td>
        ///                         <td class="data-col3 Ta(start) Pstart(20px) Miw(60px)">
        /// Industry/Category =         <a href = "https://finance.yahoo.com/sector/industrials" title="Industrials" data-symbol="LT.NS" class="Fw(b)">Industrials</a>
        ///                         </td>
        /// Type =                  <td class="data-col4 Ta(start) Pstart(20px) Miw(30px)">Stocks</td>
        /// Exchange =              <td class="data-col5 Ta(start) Pstart(20px) Pend(6px) W(30px)">NSI</td>
        ///                     </tr>
        ///                 </tbody>
        ///             </table>
        ///         </div>
        ///     </div>
        /// </div><tbody> </tbody>
        /// </summary>
        /// <param name="searchStr"></param>
        /// <param name="qualifier">Send specific qualifier to extract specific type of entity. Valid ones are - all, index</param>
        /// <param name="sqlite_cmd"></param>
        /// <returns></returns>
        public bool SearchOnlineInsertInDB(string searchStr, string qualifier = "all", SQLiteCommand sqlite_cmd = null)
        {
            bool breturn = false;
            string responseStr = null, dataStr = null;
            //WebClient webClient = null;
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            int startIndex = 0;
            int endIndex = 0;
            XmlDocument xmlResult = null;
            string compname, exchange, symbol, type, lasttradeprice, category;
            try
            {
                string webservice_url = "";

                //https://finance.yahoo.com/lookup/all?s=larsen
                webservice_url = string.Format(StockManager.urlSearch, qualifier, searchStr);
                //webClient = new System.Net.WebClient();
                //byte[] response = webClient.DownloadData(webservice_url);
                //responseXML = new StringBuilder( System.Text.UTF8Encoding.UTF8.GetString(response));
                //webClient.Dispose();
                //webClient = null;

                Uri url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                responseStr = reader.ReadToEnd();
                reader.Close();
                if (receiveStream != null)
                    receiveStream.Close();
                if (responseStr.Contains("<span>All (0)</span>") == false)
                {
                    startIndex = responseStr.IndexOf("<tbody>");
                    endIndex = responseStr.IndexOf("</tbody>") + 7;
                    if (startIndex > 0 && endIndex > 0)
                    {
                        dataStr = responseStr.Substring(startIndex, endIndex - startIndex + 1);
                        xmlResult = new XmlDocument();
                        xmlResult.LoadXml(dataStr);
                        for (int i = 0; i < xmlResult["tbody"].ChildNodes.Count; i++)
                        {
                            //get the data that we are interested in
                            //compname = xmlResult["tbody"].ChildNodes[i].ChildNodes[0].ChildNodes[0].Attributes["title"].Value; //= "LARSEN AND TOUBRO"
                            symbol = xmlResult["tbody"].ChildNodes[i].ChildNodes[0].ChildNodes[0].Attributes["data-symbol"].Value.ToUpper(); // = "LTI.NS"
                            compname = xmlResult["tbody"].ChildNodes[i].ChildNodes[1].ChildNodes[0].Value.ToUpper(); //= "LARSEN AND TOUBRO"
                            lasttradeprice = xmlResult["tbody"].ChildNodes[i].ChildNodes[2].ChildNodes[0].Value; // = "6034.15"
                            category = xmlResult["tbody"].ChildNodes[i].ChildNodes[3].ChildNodes[0].Value; // = "Technology"
                            type = xmlResult["tbody"].ChildNodes[i].ChildNodes[4].ChildNodes[0].Value; // = "Stocks"
                            exchange = xmlResult["tbody"].ChildNodes[i].ChildNodes[5].ChildNodes[0].Value; // = "NSI"
                            InsertNewStock(symbol, exchange, compname, type, lastupdt: DateTime.Today.ToString("yyyy-MM-dd"));
                        }
                        breturn = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                breturn = false;
            }
            return breturn;
        }
        /// <summary>
        /// Finds current quote from yahoo finance for the given symbol
        /// If found inserts the symbol into STOCKMASTER using symbol, exchangename and instrumenttype
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="sqlite_cmd"></param>
        /// <returns>return datatable containing SYMBOL, EXCHANGE, COMP_NAME or null</returns>
        public DataTable InsertNewStockIfNotFoundInDB(string symbol, SQLiteCommand sqlite_cmd = null)
        {
            DataTable resultDataTable = null;
            try
            {
                string webservice_url = "";
                WebResponse wr;
                Stream receiveStream = null;
                StreamReader reader = null;
                //DataRow r;

                //https://query1.finance.yahoo.com/v7/finance/chart/HDFC.BO?range=1m&interval=1m&indicators=quote&timestamp=true
                webservice_url = string.Format(StockManager.urlGlobalQuote, symbol);

                Uri url = new Uri(webservice_url);
                var webRequest = WebRequest.Create(url);
                webRequest.Method = "GET";
                webRequest.ContentType = "application/json";
                wr = webRequest.GetResponseAsync().Result;
                receiveStream = wr.GetResponseStream();
                reader = new StreamReader(receiveStream);
                string stockdata = reader.ReadToEnd();
                reader.Close();
                if (receiveStream != null)
                    receiveStream.Close();

                if (InsertStockFromJSON(stockdata, symbol, sqlite_cmd))
                {
                    resultDataTable = SearchStock(symbol, sqlite_cmd: sqlite_cmd);
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
        /// Method to fetch stock historic or current price & volume data and return the JSON data in StingBuilder
        /// </summary>
        /// <param name="scriptname">Symbol of the script with exchange code, BAJAJ.NS or BAJAJ.BO</param>
        /// <param name="range">range in days or minute to fetch data. 
        /// Valid range - [1m, 5m, 15m, 30m, 60m, "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max" ]</param>
        /// <param name="interval">What interval to be used to fetch price data. If 1m is passed then price for minute interval for the given range will be fetched. 
        /// If 1d is passed then price for each day for the range will be fetched</param>
        /// <param name="indicators"></param>
        /// <param name="includeTimestamps"></param>
        /// <returns>StringBuilder having JSON stock data</returns>
        public StringBuilder FetchStockDataOnline(string scriptname, string range, string interval, string indicators = "quote", bool includeTimestamps = true)
        {
            StringBuilder returnData = null;
            string webservice_url = "";
            WebResponse wr;
            Stream receiveStream = null;
            StreamReader reader = null;
            try
            {
                //https://query1.finance.yahoo.com/v7/finance/chart/HDFC.BO?range=2yr&interval=1d&indicators=quote&includeTimestamps=true
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
                Console.WriteLine(ex.Message);
            }
            return returnData;
        }


        /// <summary>
        /// Method to download & save stock price - open, high, low, close, volume
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
        public bool FetchOnlineAndSaveDailyStockData(string scriptname, string exchangeName = "", string equitytype = "EQ", string outputsize = "compact",
                                                    string time_interval = "1d", SQLiteCommand sqlite_cmd = null)
        {
            bool breturn = true;
            DateTime datetimeMaxTimestamp;// = time_interval.Contains("m") ? DateTime.Now : DateTime.Today;
            DateTime datetimeToday; //= time_interval.Contains("m") ? DateTime.Now : DateTime.Today;
            string range;
            int compare = -1;
            string maxTimestamp;
            //this is daily for the given script name first find from database if given time interval data exists. If it does not then find the last
            //timestamp and get data from that point till now
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null;
            SQLiteTransaction transaction = null;

            try
            {
                if (time_interval.Contains("m"))
                {
                    //means we need to get time interval data
                    datetimeToday = DateTime.Now;
                    datetimeMaxTimestamp = System.Convert.ToDateTime(DateTime.Today.ToShortDateString() + " 08:00:00");
                }
                else
                {
                    //we need to get day interval data
                    datetimeToday = DateTime.Today;
                    datetimeMaxTimestamp = DateTime.Today;
                }

                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                sqlite_cmd.CommandText = "SELECT max(timestamp) as MAXTIMESTAMP FROM STOCKDATA WHERE SYMBOL = '" + scriptname + "' AND DATA_GRANULARITY = '" + time_interval + "'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    //if (sqlite_datareader.HasRows)
                    if (sqlite_datareader.Read())
                    {
                        //sqlite_datareader.Read();
                        maxTimestamp = sqlite_datareader["MAXTIMESTAMP"].ToString();
                        sqlite_datareader.Close();
                        if (maxTimestamp != string.Empty)
                        {
                            datetimeMaxTimestamp = System.Convert.ToDateTime(maxTimestamp);

                            //we want to check if the lasttimestamp in DB is same as today or not
                            //compare returns < 0 if dt1 earlier than dt2, =0 if dt1 = dt2, > 0 dt1 later than dt2
                            compare = DateTime.Compare(datetimeMaxTimestamp, datetimeToday);
                        }
                        //we want to fetch data when compare < 0
                        if (compare < 0)
                        {
                            TimeSpan diffSpan = datetimeToday - datetimeMaxTimestamp;
                            if (string.IsNullOrEmpty(maxTimestamp))
                            {
                                range = GetRange(time_interval, outputsize);
                            }
                            else if (time_interval.Contains("d"))
                            {
                                range = diffSpan.Days.ToString() + "d";
                            }
                            else
                            {
                                if(diffSpan.Days == 0)
                                {
                                    //this means we are trying to get intra for a day
                                    range = "1d";
                                }
                                else if (time_interval == "60m") 
                                {
                                    if(diffSpan.Days >= 730)
                                        range = "2y";
                                    else
                                        range = diffSpan.Days.ToString() + "d";
                                }
                                else if (time_interval == "1m")
                                {
                                    if (diffSpan.Days > 7)
                                        range = "7d";
                                    else
                                        range = diffSpan.Days.ToString() + "d";
                                }
                                else //if ((time_interval == "15m") || (time_interval == "30m") || (time_interval == "5m"))
                                {
                                        if (diffSpan.Days >= 60)
                                        range = "60d";
                                    else
                                        range = diffSpan.Days.ToString() + "d";
                                }
                            }
                            //we need to fetch data starting from lasttimestamp. We need to send converted script name with either .NS or .BO
                            StringBuilder sbStockData = FetchStockDataOnline(scriptname, range, time_interval, indicators, true);
                            if (sbStockData != null)
                            {
                                breturn = InsertStockData(sbStockData, scriptname, exchangeName, equitytype, time_interval, sqlite_cmd);
                            }
                        }
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getStockMaster: " + exSQL.Message);
                }
                finally
                {
                    if (sqlite_conn != null)
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
                        sqlite_datareader = null;
                        sqlite_cmd = null;
                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                        sqlite_conn = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseTable: " + ex.Message);
            }

            return breturn;
        }
        #endregion

        #region DBOPERATIONS
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

        public DataTable GetInvestmentTypeList()
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = "SELECT DISTINCT(SERIES) FROM STOCKMASTER";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("GetInvestmentTypeList: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("GetInvestmentTypeList: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
                }
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
            return returnTable;
        }
        public DataTable GetExchangeList()
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = "SELECT DISTINCT(EXCHANGE) FROM STOCKMASTER";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("GetExchangeList: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getStockMaster: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
                }
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
            return returnTable;
        }

        public bool InsertNewStock(string symbol, string exchange, string company = "", string series = "EQ", string dateoflisting = "", string paidupvalue = "",
            string marketlot = "", string isinnumber = "", string facevalue = "", string lastupdt = "", SQLiteCommand sqlite_cmd = null)
        {
            bool bReturn = true;
            SQLiteConnection sqlite_conn = null;
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
                    sqlite_cmd.CommandText = "INSERT OR IGNORE INTO  STOCKMASTER(EXCHANGE, SYMBOL, COMP_NAME, SERIES, DATE_OF_LISTING, PAID_UP_VALUE, MARKET_LOT, ISIN_NUMBER, " +
                        "FACE_VALUE, LASTUPDT) " +
                   "VALUES (@EXCHANGE, @SYMBOL, @COMP_NAME, @SERIES, @DATE_OF_LISTING, @PAID_UP_VALUE, @MARKET_LOT, @ISIN_NUMBER, @FACE_VALUE, @LASTUPDT) ";
                    //"ON CONFLICT DO NOTHING";

                    sqlite_cmd.Prepare();
                    sqlite_cmd.Parameters.AddWithValue("@EXCHANGE", exchange);
                    sqlite_cmd.Parameters.AddWithValue("@SYMBOL", symbol);
                    sqlite_cmd.Parameters.AddWithValue("@COMP_NAME", company);
                    sqlite_cmd.Parameters.AddWithValue("@SERIES", series);
                    sqlite_cmd.Parameters.AddWithValue("@DATE_OF_LISTING", dateoflisting);
                    sqlite_cmd.Parameters.AddWithValue("@PAID_UP_VALUE", paidupvalue);
                    sqlite_cmd.Parameters.AddWithValue("@MARKET_LOT", marketlot);
                    sqlite_cmd.Parameters.AddWithValue("@ISIN_NUMBER", isinnumber);
                    sqlite_cmd.Parameters.AddWithValue("@FACE_VALUE", facevalue);
                    sqlite_cmd.Parameters.AddWithValue("@LASTUPDT", lastupdt);

                    sqlite_cmd.ExecuteNonQuery();

                }
                catch (SQLiteException sqlEx)
                {
                    Console.WriteLine("Error in InserNewExchange :" + sqlEx.Message);
                    bReturn = false;
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
            catch (Exception ex)
            {
                Console.WriteLine("Error in InsertNewExchange: " + ex.Message);
                bReturn = false;
            }
            return bReturn;
        }

        public DataTable SearchStock(string symbol, string exchangeCode = "", SQLiteCommand sqlite_cmd = null)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteTransaction transaction = null;
            //SQLiteCommand sqlite_cmd = null;

            try
            {
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                if ((exchangeCode.Equals(string.Empty)) || (exchangeCode.Equals("-1")))
                {
                    sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME, SERIES FROM STOCKMASTER WHERE SYMBOL = '" + symbol + "'";

                }
                else
                {
                    sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME, SERIES FROM STOCKMASTER WHERE SYMBOL = '" + symbol + "' AND EXCHANGE = '" + exchangeCode + "'";

                }
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("searchStock: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getFundHouseTable: " + ex.Message);
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
            return returnTable;
        }

        public DataTable getStockMaster(string exchangeCode = "")
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

                if ((exchangeCode.Equals(string.Empty)) || (exchangeCode.Equals("-1")))
                {
                    sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME, SERIES FROM STOCKMASTER";
                }
                else
                {
                    sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME, SERIES FROM STOCKMASTER WHERE EXCHANGE = '" + exchangeCode + "'";
                }

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
                Console.WriteLine("getStockMaster: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
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
            return returnTable;
        }

        public DataTable getIndexMaster()
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

                sqlite_cmd.CommandText = "SELECT ROWID, EXCHANGE, SYMBOL, COMP_NAME, SERIES FROM STOCKMASTER WHERE SERIES = 'Index'";

                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getIndexMaster: " + exSQL.Message);
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
                Console.WriteLine("getIndexMaster: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
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
            return returnTable;
        }

        public DataTable getExchangeMasterForIndex()
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;
            SQLiteTransaction transaction = null;
            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                transaction = sqlite_conn.BeginTransaction();
                sqlite_cmd.CommandText = "SELECT DISTINCT(EXCHANGE) FROM STOCKMASTER WHERE SERIES = 'Index'";
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("GetExchangeList: " + exSQL.Message);
                    if (returnTable != null)
                    {
                        returnTable.Clear();
                        returnTable.Dispose();
                        returnTable = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getStockMaster: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
                }
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
            return returnTable;
        }

        public bool InsertStockData(StringBuilder record, string symbol, string exchangename = "", string type = "EQUITY", string time_interval = "1d",
            SQLiteCommand sqlite_cmd = null)
        {
            bool breturn = true;
            DateTime myDate;
            double close;
            double high;
            double low;
            double open;
            long volume;
            double adjusetedClose = 0.00;
            string insertDtTm;


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

                            sqlite_cmd.CommandText = "INSERT OR IGNORE INTO  STOCKDATA(SYMBOL, EXCHANGENAME, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, TIMESTAMP) " +
                           "VALUES (@SYMBOL, @EXCHANGENAME, @TYPE, @DATA_GRANULARITY, @OPEN, @HIGH, @LOW, @CLOSE, @ADJ_CLOSE, @VOLUME, @TIMESTAMP) ";
                            //"ON CONFLICT DO NOTHING";

                            sqlite_cmd.Prepare();
                            sqlite_cmd.Parameters.AddWithValue("@SYMBOL", myMeta.symbol); //symbol);
                            sqlite_cmd.Parameters.AddWithValue("@EXCHANGENAME", myMeta.exchangeName);// exchangename);
                            sqlite_cmd.Parameters.AddWithValue("@TYPE", myMeta.instrumentType); // type);
                            sqlite_cmd.Parameters.AddWithValue("@DATA_GRANULARITY", time_interval);
                            sqlite_cmd.Parameters.AddWithValue("@OPEN", open);
                            sqlite_cmd.Parameters.AddWithValue("@HIGH", high);
                            sqlite_cmd.Parameters.AddWithValue("@LOW", low);
                            sqlite_cmd.Parameters.AddWithValue("@CLOSE", close);
                            sqlite_cmd.Parameters.AddWithValue("@ADJ_CLOSE", adjusetedClose);
                            sqlite_cmd.Parameters.AddWithValue("@VOLUME", volume);

                            insertDtTm = (time_interval.Contains("d") ? myDate.ToString("yyyy-MM-dd") : myDate.ToString("yyyy-MM-dd HH:mm:ss"));

                            sqlite_cmd.Parameters.AddWithValue("@TIMESTAMP", insertDtTm);
                            //sqlite_cmd.Parameters.AddWithValue("@TIMESTAMP", (time_interval.Contains("d") ? myDate.ToString("dd-MM-yyyy") : myDate.ToString("dd-MM-yyyy hh:mm:ss")));

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
        /// Method to fetch daily or minute wise data from DB
        /// It will call FetchOnlineAndSaveDailyStockData method to first check if we have latest data if not fetch online & insert the data in DB
        /// It will then select stock data from DB and return it in 
        ///returnTable.Columns.Add("DAILYID", typeof(long));
        ////returnTable.Columns.Add("SYMBOL", typeof(string));
        ///            returnTable.Columns.Add("EXCHANGE", typeof(string));
        ///            returnTable.Columns.Add("TYPE", typeof(string));
        ///            returnTable.Columns.Add("DATA_GRANULARITY", typeof(string));
        ///            returnTable.Columns.Add("OPEN", typeof(decimal));
        ///            returnTable.Columns.Add("HIGH", typeof(decimal));
        ///            returnTable.Columns.Add("LOW", typeof(decimal));
        ///            returnTable.Columns.Add("CLOSE", typeof(decimal));
        ///            returnTable.Columns.Add("ADJ_CLOSE", typeof(decimal));
        ///            returnTable.Columns.Add("VOLUME", typeof(decimal));
        ///            returnTable.Columns.Add("TIMESTAMP", typeof(DateTime));
        /// </summary>
        /// <param name="symbol">Symbol to return data for</param>
        /// <param name="exchangename">Exchange name, BSE or NSE to which this symbol belongs</param>
        /// <param name="type">TYpe of investment, EQ by default</param>
        /// <param name="time_interval">What is the interval for which we are seeking the stock data</param>
        /// <param name="sqlite_cmd"></param>
        /// <returns>DatatTable filled with stock data else null</returns>
        public DataTable GetStockPriceData(string symbol, string exchangename = "", string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
                                            string fromDate = null, SQLiteCommand sqlite_cmd = null)
        {
            DataTable returnTable = null;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteTransaction transaction = null;

            try
            {
                //first check if we have latest data in DB if not fetch it
                if (sqlite_cmd == null)
                {
                    sqlite_conn = CreateConnection();
                    sqlite_cmd = sqlite_conn.CreateCommand();
                    transaction = sqlite_conn.BeginTransaction();
                }
                try
                {
                    FetchOnlineAndSaveDailyStockData(symbol, exchangename, outputsize: outputsize, time_interval: time_interval, sqlite_cmd: sqlite_cmd);

                    sqlite_cmd.CommandText = "SELECT ROWID as DAILYID, SYMBOL, EXCHANGENAME as EXCHANGE, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, ";
                    if (time_interval.Contains("d"))
                    {
                        //"strftime(\"%Y-%m-%d\", TIMESTAMP) as TIMESTAMP " +
                        sqlite_cmd.CommandText += "strftime(\"%d-%m-%Y\", TIMESTAMP) as TIMESTAMP " +
                            "FROM STOCKDATA WHERE SYMBOL = '" + symbol + "' AND DATA_GRANULARITY = '" + time_interval + "' ";
                        if (fromDate != null)
                        {
                            //sqlite_cmd.CommandText += "AND strftime(\"%d-%m-%Y\", TIMESTAMP) >= '" + fromDate + "' ";
                            sqlite_cmd.CommandText += " AND TIMESTAMP >= '" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd") + "' ";
                        }
                    }
                    else
                    {
                        //"strftime(\"%Y-%m-%d &H:%M:%s\", TIMESTAMP) as TIMESTAMP " +
                        sqlite_cmd.CommandText += "strftime(\"%d-%m-%Y %H:%M:%S\", TIMESTAMP) as TIMESTAMP " +
                            "FROM STOCKDATA WHERE SYMBOL = '" + symbol + "' AND DATA_GRANULARITY = '" + time_interval + "' ";
                        if (fromDate != null)
                        {
                            //sqlite_cmd.CommandText += " AND TIMESTAMP >= '" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd hh:mm:ss") + "' ";
                            sqlite_cmd.CommandText += " AND TIMESTAMP >= '" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd") + " 08:00:00' ";
                        }
                        else
                        {
                            //we will limit the intra day data to 1 day
                            //sqlite_cmd.CommandText += " AND TIMESTAMP >= '" + DateTime.Today.ToString("yyyy-MM-dd HH:mm:ss") + "' ";
                            sqlite_cmd.CommandText += " AND TIMESTAMP >= '" + DateTime.Today.ToString("yyyy-MM-dd") + " 08:00:00' ";
                        }
                        sqlite_cmd.CommandText += " AND TIMESTAMP <= '" + System.Convert.ToDateTime(fromDate).ToString("yyyy-MM-dd") + " 23:59:00' ";
                    }
                    if ((exchangename != string.Empty) && (exchangename.Equals("-1") == false))
                    {
                        sqlite_cmd.CommandText += " AND EXCHANGENAME = '" + exchangename + "' ";
                    }

                    //sqlite_cmd.CommandText += "ORDER BY TIMESTAMP ASC";

                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Columns.Add("DAILYID", typeof(long));
                    returnTable.Columns.Add("SYMBOL", typeof(string));
                    returnTable.Columns.Add("EXCHANGE", typeof(string));
                    returnTable.Columns.Add("TYPE", typeof(string));
                    returnTable.Columns.Add("DATA_GRANULARITY", typeof(string));
                    returnTable.Columns.Add("OPEN", typeof(decimal));
                    returnTable.Columns.Add("HIGH", typeof(decimal));
                    returnTable.Columns.Add("LOW", typeof(decimal));
                    returnTable.Columns.Add("CLOSE", typeof(decimal));
                    returnTable.Columns.Add("ADJ_CLOSE", typeof(decimal));
                    returnTable.Columns.Add("VOLUME", typeof(decimal));
                    returnTable.Columns.Add("TIMESTAMP", typeof(DateTime));

                    returnTable.Load(sqlite_datareader);

                    //IEnumerable<DataRow> orderedRows = returnTable.AsEnumerable().OrderBy(r => r.Field<DateTime>("TIMESTAMP"));
                    //returnTable = orderedRows.CopyToDataTable();


                    //returnTable.DefaultView.Sort = "TIMESTAMP ASC";
                    //returnTable = returnTable.DefaultView.ToTable();
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
                finally
                {
                    if (sqlite_conn != null)
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

                        if (sqlite_cmd != null)
                        {
                            sqlite_cmd.Dispose();
                        }

                        sqlite_conn.Close();
                        sqlite_conn.Dispose();
                        sqlite_datareader = null;
                        transaction = null;
                        sqlite_cmd = null;
                        sqlite_conn = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getStockMaster: " + ex.Message);
                if (returnTable != null)
                {
                    returnTable.Clear();
                    returnTable.Dispose();
                    returnTable = null;
                }
            }
            return returnTable;
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

        /// <summary>
        /// Method to update given portfolio transaction row with supplied data
        /// </summary>
        /// <param name="stockportfolioRowId">ROWID of the transaction from STOCKPORTFOLIO table</param>
        /// <param name="purchasePrice"></param>
        /// <param name="purchaseDate"></param>
        /// <param name="purchaseQty"></param>
        /// <param name="commissionPaid"></param>
        /// <param name="totalCost"></param>
        /// <returns>trus if update is successfull else false</returns>
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

        #endregion

        #region graph methods
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
        public DataTable GetSMA_EMA_MACD_BBANDS_Table(string symbol, string exchangename = "", string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
                                    string fromDate = null, int small_fast_Period = 10, int long_slow_Period = -1, bool emaRequired = false, bool macdRequired = false,
                                    //string fastperiod = "12", string slowperiod = "26", 
                                    int signalperiod = 9, bool bbandsRequired = false, int stddeviation = 2, SQLiteCommand sqlite_cmd = null)
        {
            DataTable dailyTable = null;
            try
            {
                //DAILYID, SYMBOL, EXCHANGE, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, TIMESTAMP
                dailyTable = GetStockPriceData(symbol, exchangename, seriestype, outputsize, time_interval, fromDate, sqlite_cmd);

                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
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
                        {    //subtract the oldest CLOSE PRICE from the previous SUM and then add the current CLOSE PRICE
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
                            dailyTable.Rows[subrownum - 1]["Middle Band"] = Math.Round(System.Convert.ToDouble(dailyTable.Rows[subrownum - 1]["SMA_SMALL"]), 2);
                            dailyTable.Rows[subrownum - 1]["Upper Band"] = Math.Round(upperBand, 4);
                        }
                    }
                    if (emaRequired == true)
                    {
                        newCol = new DataColumn("EMA_SMALL", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("EMA_LONG", typeof(decimal));
                        newCol.DefaultValue = 0;
                        dailyTable.Columns.Add(newCol);

                        newCol = new DataColumn("EMA_CROSSOVER_FLAG", typeof(string));
                        newCol.DefaultValue = "LT";
                        dailyTable.Columns.Add(newCol);

                        //we will not have EMA for initial 0 to smallperiod rows. But EMA starts with smallPeriod row = SMA  for that period

                        double multiplier = 2 / ((double)small_fast_Period + 1);
                        double ema = 0.00;
                        double currentPrice = 0;
                        double prevEMA = 0;
                        if (small_fast_Period > 0)
                        {    //iterate through table starting from smallperiod - 1 ie. if period is 10 then we use SMA for 9th index in table and then start calculating ema
                            //we have already set ema for 9th index above so we can start the loop from 10th index
                            dailyTable.Rows[small_fast_Period - 1]["EMA_SMALL"] = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[small_fast_Period - 1]["SMA_SMALL"]));
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
        public DataTable GetBacktestFromSMA(string symbol, string exchangename = "", string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
                                            string fromDate = null, int smallPeriod = 10, int longPeriod = 20, int buySpan = 2, int sellSpan = 20,
                                            double simulationQty = 100)
        {
            DataTable dailyTable = null;
            double buyPrice = 0.00, sellPrice = 0.00;
            double buyCost = 0.00;
            double sellValue = 0.00;
            double profit_loss = 0.00;
            StringBuilder resultString = new StringBuilder();
            try
            {
                dailyTable = GetSMA_EMA_MACD_BBANDS_Table(symbol, exchangename, seriestype, outputsize, time_interval, fromDate, smallPeriod, longPeriod, sqlite_cmd: null);
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


                                    buyPrice = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[i + buySpan + 1]["CLOSE"]));
                                    buyCost = Math.Round((simulationQty * buyPrice), 2);


                                    //set start point
                                    //dailyTable.Rows[i]["CROSSOVER_FLAG"] = "X";
                                    dailyTable.Rows[i + buySpan]["CROSSOVER_FLAG"] = "X";

                                    //set crossover point for buy
                                    dailyTable.Rows[i + buySpan + 1]["BUY_FLAG"] = true;
                                    dailyTable.Rows[i + buySpan + 1]["QUANTITY"] = simulationQty;
                                    dailyTable.Rows[i + buySpan + 1]["BUY_COST"] = buyCost;

                                    //Current logic for setting sell_flag is simple, but it needs to be changed to reflect following
                                    // From the point where we marked buy flag = true (this point specifies sma small < sma long) upto sell span days
                                    //  check for each point where crossover flag == GT (meaning sma small > smal long)
                                    //      if a point is found then mark it as Sell Flag = true
                                    //          
                                    if ((i + sellSpan) < dailyTable.Rows.Count)
                                    {
                                        //set sell point
                                        sellPrice = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[i + sellSpan]["CLOSE"]));
                                        sellValue = Math.Round((simulationQty * sellPrice), 2);
                                        profit_loss = Math.Round((sellValue - buyCost), 2);

                                        dailyTable.Rows[i + sellSpan]["QUANTITY"] = simulationQty;
                                        dailyTable.Rows[i + sellSpan]["SELL_FLAG"] = true;
                                        dailyTable.Rows[i + sellSpan]["SELL_VALUE"] = sellValue;
                                        dailyTable.Rows[i + sellSpan]["PROFIT_LOSS"] = profit_loss;

                                        resultString.AppendLine("BUY Date: " + System.Convert.ToDateTime(dailyTable.Rows[i + buySpan + 1]["TIMESTAMP"]).ToString("dd-MM-yyyy"));
                                        resultString.AppendLine("Simulation Qty: " + simulationQty);
                                        resultString.AppendLine("BUY Price: " + buyPrice);
                                        resultString.AppendLine("BUY Cost: " + buyCost);
                                        resultString.AppendLine("SELL Date: " + System.Convert.ToDateTime(dailyTable.Rows[i + sellSpan]["TIMESTAMP"]).ToString("dd-MM-yyyy"));
                                        resultString.AppendLine("SELL Price: " + sellPrice);
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
                Console.WriteLine("GetBackTestFromSMA exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }

        public DataTable getRSIDataTableFromDaily(string symbol, string exchange, string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
            string fromDate = null, string period = "14", bool stochRSI = false)
        {
            DataTable dailyTable = null;
            //DataTable rsiDataTable = null;
            int iPeriod;
            double change, gain, loss, avgGain = 0.00, avgLoss = 0.00, rs, rsi;
            double sumOfGain = 0.00, sumOfLoss = 0.00;
            //DateTime dateCurrentRow = DateTime.Today;
            List<string> seriesNameList;
            try
            {
                dailyTable = GetStockPriceData(symbol, exchange, seriestype, outputsize, time_interval, fromDate, sqlite_cmd: null);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    iPeriod = System.Convert.ToInt32(period);
                    DataColumn newCol;
                    if (stochRSI == false)
                    {
                        newCol = new DataColumn("RSI_" + seriestype, typeof(decimal));
                        newCol.DefaultValue = 0.00;

                        dailyTable.Columns.Add(newCol);
                        seriesNameList = new List<string> { seriestype };
                    }
                    else
                    {
                        newCol = new DataColumn("RSI_OPEN", typeof(decimal));
                        newCol.DefaultValue = 0.00;

                        dailyTable.Columns.Add(newCol);
                        newCol = new DataColumn("RSI_CLOSE", typeof(decimal));
                        newCol.DefaultValue = 0.00;

                        dailyTable.Columns.Add(newCol);
                        newCol = new DataColumn("RSI_HIGH", typeof(decimal));
                        newCol.DefaultValue = 0.00;

                        dailyTable.Columns.Add(newCol);
                        newCol = new DataColumn("RSI_LOW", typeof(decimal));
                        newCol.DefaultValue = 0.00;

                        dailyTable.Columns.Add(newCol);

                        seriesNameList = new List<string> { "CLOSE", "OPEN", "HIGH", "LOW" };
                    }
                    foreach (var item in seriesNameList)
                    {
                        change = gain = loss = avgGain = avgLoss = rs = rsi = 0.00;
                        sumOfGain = sumOfLoss = 0.00;

                        for (int rownum = 1; rownum < dailyTable.Rows.Count; rownum++)
                        {
                            //current - prev
                            //change = System.Convert.ToDouble(dailyTable.Rows[rownum][seriestype]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1][seriestype]);
                            change = System.Convert.ToDouble(dailyTable.Rows[rownum][item.ToString()]) - System.Convert.ToDouble(dailyTable.Rows[rownum - 1][item.ToString()]);
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
                                //dailyTable.Rows[rownum]["RSI"] = Math.Round(rsi, 2);
                                dailyTable.Rows[rownum]["RSI_" + item.ToString()] = Math.Round(rsi, 2);
                            }
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


        public DataTable getVWAPDataTableFromDaily(string symbol, string exchange, string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
            string fromDate = null)
        {
            DataTable dailyTable = null;
            double high, low, close, avgprice = 0.00, cumavgpricevol = 0.00, vwap = 0.00, prev_cumavgpricevol = 0.00;
            DateTime transDate;
            long volume, cumvol = 0, prev_cumvol = 0;
            try
            {
                dailyTable = GetStockPriceData(symbol, exchange, seriestype, outputsize, time_interval, fromDate, sqlite_cmd: null);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    DataColumn newCol = new DataColumn("VWAP", typeof(decimal));
                    newCol.DefaultValue = 0.00;

                    dailyTable.Columns.Add(newCol);

                    for (int i = 0; i < dailyTable.Rows.Count; i++)
                    {
                        //find all the values
                        transDate = System.Convert.ToDateTime(dailyTable.Rows[i]["TIMEStAMP"]);
                        high = System.Convert.ToDouble(dailyTable.Rows[i]["HIGH"]);
                        low = System.Convert.ToDouble(dailyTable.Rows[i]["LOW"]);
                        close = System.Convert.ToDouble(dailyTable.Rows[i]["CLOSE"]);
                        volume = System.Convert.ToInt64(dailyTable.Rows[i]["VOLUME"]);
                        if (volume == 0)
                            continue;

                        avgprice = (high + low + close) / 3;

                        cumavgpricevol = (avgprice * volume) + prev_cumavgpricevol;
                        prev_cumavgpricevol = cumavgpricevol;

                        cumvol = volume + prev_cumvol;
                        prev_cumvol = cumvol;
                        vwap = cumavgpricevol / cumvol;
                        dailyTable.Rows[i]["VWAP"] = Math.Round(vwap, 2);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getVWAPDataTableFromDaily exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }


        public DataTable getADX_DX_DM_DI_DataTableFromDaily(string symbol, string exchange, string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
            string fromDate = null, string period = "20")
        {
            DataTable dailyTable = null;
            int iPeriod;
            DateTime dateLastRow = DateTime.Today;

            double tr, minusDM, plusDM, trPeriod, minusDMPeriod, plusDMPeriod, minusDIPeriod, plusDIPeriod, dx, adx;
            List<double> listTR = new List<double>();
            List<double> listPlusDM1 = new List<double>();
            List<double> listMinusDM1 = new List<double>();
            List<double> listTRPeriod = new List<double>();
            List<double> listPlusDMPeriod = new List<double>();
            List<double> listMinusDMPeriod = new List<double>();
            List<double> listPlusDIPeriod = new List<double>();
            List<double> listMinusDIPeriod = new List<double>();
            List<double> listDX = new List<double>();
            List<double> listADX = new List<double>();

            try
            {
                dailyTable = GetStockPriceData(symbol, exchange, seriestype, outputsize, time_interval, fromDate, sqlite_cmd: null);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    iPeriod = System.Convert.ToInt32(period);
                    DataColumn newCol = new DataColumn("ADX", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("DX", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("PLUS_DM", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("MINUS_DM", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("PLUS_DI", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("MINUS_DI", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    for (int rownum = 1; rownum < dailyTable.Rows.Count; rownum++)
                    //for (int rownum = 0; rownum < dailyTable.Rows.Count; rownum++)
                    {
                        dateLastRow = System.Convert.ToDateTime(dailyTable.Rows[rownum]["TIMESTAMP"]);
                        tr = FindTR1(rownum, dailyTable);
                        listTR.Add(tr);

                        plusDM = FindPositiveDM1(rownum, dailyTable);
                        listPlusDM1.Add(plusDM);

                        minusDM = FindNegativeDM1(rownum, dailyTable);
                        listMinusDM1.Add(minusDM);

                        if (rownum >= iPeriod)
                        //if ((rownum + 1) >= iPeriod)
                        {
                            //this means we have reached period number of rows or greater
                            trPeriod = FindTR_Period(rownum, iPeriod, listTR, listTRPeriod);
                            listTRPeriod.Add(trPeriod);

                            plusDMPeriod = FindPositveDM_Period(rownum, iPeriod, listPlusDM1, listPlusDMPeriod);
                            listPlusDMPeriod.Add(plusDMPeriod);

                            minusDMPeriod = FindNegativeDM_Period(rownum, iPeriod, listMinusDM1, listMinusDMPeriod);
                            listMinusDMPeriod.Add(minusDMPeriod);

                            plusDIPeriod = FindPositveDI_Period(rownum, iPeriod, listTRPeriod, listPlusDMPeriod);
                            listPlusDIPeriod.Add(plusDIPeriod);

                            minusDIPeriod = FindNegativeDI_Period(rownum, iPeriod, listTRPeriod, listMinusDMPeriod);
                            listMinusDIPeriod.Add(minusDIPeriod);

                            dx = FindDX(rownum, iPeriod, listPlusDIPeriod, listMinusDIPeriod);
                            listDX.Add(dx);

                            if ((rownum + 1) >= (iPeriod * 2))
                            //if ((rownum + 2) >= (iPeriod * 2))
                            {
                                adx = FindADX(rownum, iPeriod, listDX, listADX);
                                listADX.Add(adx);

                                dailyTable.Rows[rownum]["ADX"] = Math.Round(adx, 2);
                            }
                            dailyTable.Rows[rownum]["DX"] = Math.Round(dx, 2);
                            dailyTable.Rows[rownum]["PLUS_DM"] = Math.Round(plusDMPeriod, 2);
                            dailyTable.Rows[rownum]["MINUS_DM"] = Math.Round(minusDMPeriod, 2);
                            dailyTable.Rows[rownum]["PLUS_DI"] = Math.Round(plusDIPeriod, 2);
                            dailyTable.Rows[rownum]["MINUS_DI"] = Math.Round(minusDIPeriod, 2);
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

        public DataTable getSTOCHDataTableFromDaily(string symbol, string exchange, string seriestype = "CLOSE", string outputsize = "Compact", string time_interval = "1d",
            string fromDate = null, string fastkperiod = "5", string slowdperiod = "3", bool bRSIRequired = false, string rsiPeriod = "14")
        {
            DataTable dailyTable = null;
            //DataTable rsiDataTable = null;
            int iFastKPeriod, iSlowDPeriod;
            double slowK = 0.00, slowD = 0.00, highestHigh = 0.00, lowestLow = 0.00;
            DateTime dateCurrentRow = DateTime.Today;
            List<double> listHigh = new List<double>();
            List<double> listClose = new List<double>();
            List<double> listLow = new List<double>();
            List<double> listHighestHigh = new List<double>();
            List<double> listLowestLow = new List<double>();
            List<double> listSlowK = new List<double>();

            int startSlowK, startSlowD;
            int rsiIndexAdjustor = 0;

            try
            {
                if (bRSIRequired == true)
                {
                    dailyTable = getRSIDataTableFromDaily(symbol, exchange, seriestype, outputsize, time_interval, fromDate, rsiPeriod, stochRSI: true);
                    rsiIndexAdjustor = Int32.Parse(rsiPeriod);
                }
                else
                {
                    dailyTable = GetStockPriceData(symbol, exchange, seriestype, outputsize, time_interval, fromDate, sqlite_cmd: null);
                }

                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    iFastKPeriod = System.Convert.ToInt32(fastkperiod);
                    iSlowDPeriod = System.Convert.ToInt32(slowdperiod);

                    startSlowK = 0; startSlowD = 0;

                    DataColumn newCol = new DataColumn("SlowD", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);
                    newCol = new DataColumn("SlowK", typeof(decimal));
                    newCol.DefaultValue = 0.00;
                    dailyTable.Columns.Add(newCol);

                    for (int rownum = rsiIndexAdjustor; rownum < dailyTable.Rows.Count; rownum++)
                    {
                        if (bRSIRequired == false)
                        {
                            listClose.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["CLOSE"]));
                            listHigh.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["HIGH"]));
                            listLow.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["LOW"]));
                        }
                        else
                        {
                            listClose.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["RSI_CLOSE"]));
                            listHigh.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["RSI_HIGH"]));
                            listLow.Add(System.Convert.ToDouble(dailyTable.Rows[rownum]["RSI_LOW"]));
                        }
                        if (((rownum - rsiIndexAdjustor) + 1) >= iFastKPeriod) //CASE of iFastKPeriod = 5: rownum = 4, 5th or higher row
                        {
                            highestHigh = FindHighestHigh(listHigh, startSlowK, iFastKPeriod);
                            listHighestHigh.Add(highestHigh);

                            lowestLow = FindLowestLow(listLow, startSlowK, iFastKPeriod);
                            listLowestLow.Add(lowestLow);

                            startSlowK++;

                            slowK = FindSlowK(listClose, listHighestHigh, listLowestLow);
                            listSlowK.Add(slowK);

                            /*if (((rownum - rsiIndexAdjustor) + 1) >= (iFastKPeriod + iSlowDPeriod))*/ //CASE of iSlowDPeriod = 3: rownum = 7, 8th or higher row
                            if (((rownum - rsiIndexAdjustor) + 2) >= (iFastKPeriod + iSlowDPeriod))
                            {
                                slowD = FindSlowD(listSlowK, startSlowD, iSlowDPeriod);
                                startSlowD++;

                                //now save the datat
                                //dateCurrentRow = System.Convert.ToDateTime(dailyTable.Rows[rownum]["TIMESTAMP"]);

                                dailyTable.Rows[rownum]["SlowD"] = Math.Round(slowD, 2);
                                dailyTable.Rows[rownum]["SlowK"] = Math.Round(slowK, 2);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getSTOCHDataTableFromDaily exception: " + ex.Message);

                if (dailyTable != null)
                {
                    dailyTable.Clear();
                    dailyTable.Dispose();
                }
                dailyTable = null;
            }
            return dailyTable;
        }

        public DataTable getAROONDataTableFromDaily(string symbol, string exchange, string seriestype = "CLOSE", string outputsize = "Compact",
            string time_interval = "1d", string fromDate = null, int period = 20)
        {
            DataTable dailyTable = null;
            double aroonUp, aroonDown;
            DateTime dateLastRow = DateTime.Today;
            try
            {
                dailyTable = GetStockPriceData(symbol, exchange, seriestype, outputsize, time_interval, fromDate, sqlite_cmd: null);
                if ((dailyTable != null) && (dailyTable.Rows.Count > 0))
                {
                    DataColumn newCol = new DataColumn("Aroon Down", typeof(decimal));
                    newCol.DefaultValue = 0.00;

                    dailyTable.Columns.Add(newCol);

                    newCol = new DataColumn("Aroon Up", typeof(decimal));
                    newCol.DefaultValue = 0.00;

                    dailyTable.Columns.Add(newCol);

                    for (int rownum = period; rownum < dailyTable.Rows.Count; rownum++)
                    {
                        //find all the values
                        aroonUp = CalculateAroonUp(rownum, period, dailyTable);
                        aroonDown = CalculateAroonDown(rownum, period, dailyTable);
                        dailyTable.Rows[rownum]["Aroon Down"] = Math.Round(aroonDown, 2);
                        dailyTable.Rows[rownum]["Aroon Up"] = Math.Round(aroonUp, 2);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getAROONDataTableFromDaily exception: " + ex.Message);

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
        /// <summary>
        /// Valid intervals: [1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo]
        /// valid range for each interval
        /// [1m = 7d, 2m = 5m = 15m = 30m = 60d, 60m = 730d (or 2y), 90m = 60d, 1h = 730d (or 2y), 1d = 5d = 1wk = 1mo = 3mo = any range from ["1d","5d","1mo","3mo","6mo","1y","2y","5y","10y","ytd","max"]]
        /// </summary>
        /// <param name="time_interval"></param>
        /// <param name="outputsize"></param>
        /// <returns></returns>
        public string GetRange(string time_interval, string outputsize)
        {
            StringBuilder range = new StringBuilder();
            if (time_interval.Equals("1d"))
            {
                //if (outputsize.Equals("Compact"))
                //{
                //    range.Append("3mo");
                //}
                //else //if (outputsize.Equals("compact"))
                //{
                //    range.Append("10y");
                //}
                range.Append("10y");
            }
            else if (time_interval == "60m")
            {
                //if (outputsize.Equals("Compact"))
                //{
                //    range.Append("1d");
                //}
                //else
                //{
                //    range.Append("2y");
                //}
                range.Append("2y");

            }
            else if (time_interval == "1m")
            {
                //if (outputsize.Equals("Compact"))
                //{
                //    range.Append("1d");
                //}
                //else
                //{
                //    range.Append("7d");
                //}
                range.Append("7d");
            }
            else //if ((time_interval == "15m") || (time_interval == "30m") || (time_interval == "5m"))
            {
                //if (outputsize.Equals("Compact"))
                //{
                //    range.Append("1d");
                //}
                //else
                //{
                //    range.Append("60d");
                //}
                range.Append("60d");
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

        public long convertDateTimeToUnixEpoch(DateTime dtToConvert)
        {
            DateTimeOffset dtoffset = new DateTimeOffset(new DateTime(dtToConvert.Year, dtToConvert.Month, dtToConvert.Day, 0, 0, 0, DateTimeKind.Utc));

            return dtoffset.ToUnixTimeSeconds();
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




        #region PORTFOLIO
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
                    sqlite_cmd.CommandText = "INSERT OR IGNORE INTO STOCKPORTFOLIO_MASTER(USERID, PORTFOLIO_NAME) VALUES (@USERID, @NAME)";
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
                    //To make sure the data gets saved with time use the following format
                    //sqlite_cmd.Parameters.AddWithValue("@PURCHASE_DATE", System.Convert.ToDateTime(date).ToString("yyyy-MM-dd hh:mm:ss"));
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
        /// For given STOCKMASTERPORTFOLIO ROW ID, this method deletes portfolio rows from StOCKPORTFOLIO & row from STOCKMASTERPORTFOLIO
        /// </summary>
        /// <param name="masterstockportfolio_rowid">ROWID of the MASTERSTOCKPORTFOLIO</param>
        /// <returns>true if row deleted data for the given portfolio successfully else false</returns>
        public bool DeletePortfolio(string masterstockportfolio_rowid)
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
                    sqlite_cmd.CommandText = "DELETE FROM STOCKPORTFOLIO WHERE MASTER_ROWID = " + masterstockportfolio_rowid;

                    try
                    {
                        if (sqlite_cmd.ExecuteNonQuery() >= 0)
                        {
                            sqlite_cmd.CommandText = "DELETE FROM STOCKPORTFOLIO_MASTER WHERE ROWID = " + masterstockportfolio_rowid;
                            if (sqlite_cmd.ExecuteNonQuery() > 0)
                            {
                                breturn = true;
                            }
                        }
                    }
                    catch (SQLiteException exSQL)
                    {
                        Console.WriteLine("deletePortfolioRow: [" + masterstockportfolio_rowid + "] " + exSQL.Message);
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

            if ((userid == null) || (userid.Equals(string.Empty)))
                return returnTable;
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

        public DataTable getSymbolListFromPortfolio(string portfolioMasterId)
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
                sqlite_cmd.CommandText = "SELECT STOCKMASTER.ROWID, STOCKMASTER.EXCHANGE, STOCKMASTER.SYMBOL, STOCKMASTER.COMP_NAME FROM STOCKPORTFOLIO " +
                    "INNER JOIN STOCKMASTER ON STOCKMASTER.ROWID = STOCKPORTFOLIO.STOCKMASTER_ROWID " +
                    "WHERE STOCKPORTFOLIO.MASTER_ROWID = " + portfolioMasterId +
                    " GROUP BY STOCKPORTFOLIO.STOCKMASTER_ROWID";// +
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    returnTable = new DataTable();
                    returnTable.Load(sqlite_datareader);
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine(exSQL.Message);
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
                Console.WriteLine(ex.Message);
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
        /// Method to give table that shows valuation of each script.
        /// It uses historic daily stock data and merges the portfolio table
        /// </summary>
        /// <param name="portfolioMasterRowId"></param>
        /// <returns></returns>

        public DataTable GetPortfolio_ValuationLineGraph(string portfolioMasterId, string interval = "1d")
        {
            DataTable valuationTable = null;
            DataTable portfolioSummaryTable;
            DataTable portfolioTransactionTable;
            DataTable dailyTable;

            DateTime currentTxnDate;
            DateTime nextTxnDate;
            DateTime currentPriceDate;
            DateTime firstPurchaseDate;

            double purchaseCommission = 0, purchasePrice = 0, purchaseQty = 0, currentPrice = 0, currentValue = 0, investmentCost = 0.00, cumQty = 0, cumCost = 0, cumValue = 0.00, yearsInvested = 0.00, arr = 0.00;
            double totalYearsInvested = 0.00, totalARR = 0.00;

            int dailyRowNum = 0;

            bool bPortfolioFlag = true;
            bool blasttxn = false;
            SQLiteConnection sqlite_conn = null;
            SQLiteDataReader sqlite_datareader = null; ;
            SQLiteCommand sqlite_cmd = null;

            string statement;

            try
            {
                //First get the symbol & their first & last purchase date & cumulative units
                statement = "SELECT STOCKMASTER.ROWID as STOCKMASTER_ROWID, STOCKMASTER.EXCHANGE, STOCKMASTER.SYMBOL, " +
                    //"min(STOCKPORTFOLIO.PURCHASE_DATE) as FirstPurchaseDate, max(STOCKPORTFOLIO.PURCHASE_DATE) as LastPurchaseDate, " +
                    "strftime('%d-%m-%Y', min(STOCKPORTFOLIO.PURCHASE_DATE)) as FirstPurchaseDate, " +
                    "strftime('%d-%m-%Y', max(STOCKPORTFOLIO.PURCHASE_DATE)) as LastPurchaseDate, " +
                    "sum(STOCKPORTFOLIO.PURCHASE_QTY) as SUMUNITS FROM STOCKPORTFOLIO " +
                    "INNER JOIN STOCKMASTER ON STOCKMASTER.ROWID = STOCKPORTFOLIO.STOCKMASTER_ROWID " +
                    "WHERE STOCKPORTFOLIO.MASTER_ROWID = " + portfolioMasterId +
                    " GROUP BY STOCKPORTFOLIO.STOCKMASTER_ROWID ";// +
                                                                  //"ORDER BY STOCKPORTFOLIO.STOCKMASTER_ROWID ASC, FirstPurchaseDate ASC";

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
                    sqlite_datareader = null;

                    //NOw we have name of each symbol & first purchase date from the portfolio table
                    //Now get daily price records for each symbol and build valuation table.
                    //We also mark & identify a row where PURCHASE_DATE & CURRENT_DATE is same as portfolio transacton
                    foreach (DataRow summaryRow in portfolioSummaryTable.Rows)
                    {
                        firstPurchaseDate = System.Convert.ToDateTime(summaryRow["FirstPurchaseDate"].ToString());
                        //first get the daily price records for the current symbol starting from summary rows First Purchase Date
                        dailyTable = GetStockPriceData(summaryRow["SYMBOL"].ToString(), summaryRow["EXCHANGE"].ToString(), seriestype: "CLOSE", outputsize: "Full", time_interval: "1d",
                                            fromDate: firstPurchaseDate.ToString("dd-MM-yyyy"), sqlite_cmd);
                        if ((dailyTable == null) || ((dailyTable != null) && (dailyTable.Rows.Count <= 0)))
                        {
                            continue;
                        }
                        //the above method return table having following columns
                        //DAILYID, SYMBOL, EXCHANG, TYPE, DATA_GRANULARITY, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME,
                        //sqlite_cmd.CommandText += "strftime(\"%d-%m-%Y\", TIMESTAMP) as TIMESTAMP

                        //We have to add additional columns from portfolio table as well as columns to store cumulative values
                        //following columns are for storing values used in portfolio rendering, they will come from portfolio query for each symbol
                        dailyTable.Columns.Add("ID", typeof(long)); //Portfolio query
                        dailyTable.Columns.Add("MASTERID", typeof(long)); //Portfolio query
                        dailyTable.Columns.Add("STOCKID", typeof(long)); //Portfolio query
                        //dailyTable.Columns.Add("EXCHANGE", typeof(string)); //We do not need this from portfolio query
                        //dailyTable.Columns.Add("SYMBOL", typeof(string)); //We do not this from portfolio query
                        dailyTable.Columns.Add("COMP_NAME", typeof(string)); //Portfolio query
                        dailyTable.Columns.Add("SERIES", typeof(string)); //Portfolio query
                        dailyTable.Columns.Add("PURCHASE_DATE", typeof(string)); //Portfolio query
                        dailyTable.Columns.Add("PURCHASE_PRICE", typeof(decimal)); //Portfolio query
                        dailyTable.Columns.Add("PURCHASE_QTY", typeof(decimal)); //Portfolio query
                        dailyTable.Columns.Add("COMMISSION_TAXES", typeof(decimal)); //Portfolio query
                        dailyTable.Columns.Add("INVESTMENT_COST", typeof(decimal)); //Portfolio query

                        //following columns are added to store the values calculated during runtime
                        dailyTable.Columns.Add("CURRENTVALUE", typeof(decimal)); //runtime filled using current stock price & portfolio qty
                        dailyTable.Columns.Add("YearsInvested", typeof(decimal)); //runtime filled
                        dailyTable.Columns.Add("ARR", typeof(decimal));    //runtime filled 
                        dailyTable.Columns.Add("CumulativeQty", typeof(decimal));
                        dailyTable.Columns.Add("CumulativeCost", typeof(decimal));
                        dailyTable.Columns.Add("CumulativeValue", typeof(decimal));
                        dailyTable.Columns.Add("FirstPurchaseDate", typeof(string));
                        dailyTable.Columns.Add("CumulativeYearsInvested", typeof(decimal));
                        dailyTable.Columns.Add("CumulativeARR", typeof(decimal));
                        dailyTable.Columns.Add("PORTFOLIO_FLAG", typeof(string));
                        //Now we have records from STOCKDATA table for the current scheme & the TIMESTAMP date >= first puchase date

                        //now get the transaction records for the specific SYMBOL & EXCHANGE from portfolio table
                        statement = "SELECT STOCKPORTFOLIO.ROWID as ID, STOCKPORTFOLIO.MASTER_ROWID as MASTERID, STOCKPORTFOLIO.STOCKMASTER_ROWID as STOCKID, " +
                                    "STOCKMASTER.EXCHANGE, STOCKMASTER.SYMBOL, STOCKMASTER.COMP_NAME, STOCKMASTER.SERIES, " +
                                    "strftime('%d-%m-%Y', STOCKPORTFOLIO.PURCHASE_DATE) AS PURCHASE_DATE, STOCKPORTFOLIO.PURCHASE_PRICE, STOCKPORTFOLIO.PURCHASE_QTY, " +
                                    "STOCKPORTFOLIO.COMMISSION_TAXES, STOCKPORTFOLIO.INVESTMENT_COST FROM STOCKMASTER " +
                                    "INNER JOIN STOCKPORTFOLIO ON STOCKPORTFOLIO.STOCKMASTER_ROWID = STOCKMASTER.ROWID " +
                                    "WHERE STOCKPORTFOLIO.MASTER_ROWID = " + portfolioMasterId + " AND " +
                                    "STOCKPORTFOLIO.STOCKMASTER_ROWID = " + summaryRow["STOCKMASTER_ROWID"].ToString() + " " +
                                    //"ORDER BY STOCKPORTFOLIO.STOCKMASTER_ROWID ASC, STOCKPORTFOLIO.PURCHASE_DATE ASC";
                                    "ORDER BY STOCKPORTFOLIO.PURCHASE_DATE ASC";


                        sqlite_cmd.CommandText = statement;
                        sqlite_datareader = sqlite_cmd.ExecuteReader();
                        portfolioTransactionTable = new DataTable();
                        portfolioTransactionTable.Load(sqlite_datareader);
                        sqlite_datareader.Close();

                        dailyRowNum = 0;
                        blasttxn = false;
                        purchasePrice = currentPrice = currentValue = investmentCost = cumQty = cumCost = cumValue = yearsInvested = arr = totalYearsInvested = totalARR = 0.00;

                        //Now we will find cumulative values
                        for (int txnRowNum = 0; txnRowNum < portfolioTransactionTable.Rows.Count; txnRowNum++)
                        {
                            currentTxnDate = System.Convert.ToDateTime(portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_DATE"].ToString());
                            nextTxnDate = DateTime.Today;
                            blasttxn = true;
                            if (txnRowNum + 1 < portfolioTransactionTable.Rows.Count)
                            {
                                nextTxnDate = System.Convert.ToDateTime(portfolioTransactionTable.Rows[txnRowNum + 1]["PURCHASE_DATE"].ToString());
                                blasttxn = false;
                            }

                            purchasePrice = System.Convert.ToDouble(string.Format("{0:0.00}", portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_PRICE"]));
                            purchaseQty = System.Convert.ToDouble(string.Format("{0:0.00}", portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_QTY"]));
                            purchaseCommission = System.Convert.ToDouble(string.Format("{0:0.00}", portfolioTransactionTable.Rows[txnRowNum]["COMMISSION_TAXES"]));
                            investmentCost = System.Convert.ToDouble(string.Format("{0:0.00}", portfolioTransactionTable.Rows[txnRowNum]["INVESTMENT_COST"]));

                            cumQty += purchaseQty;
                            cumCost += investmentCost;

                            bPortfolioFlag = true;
                            //Now we need to use the dailytable and add the portfolio table values & cumulative values in daily table
                            //We will use the transaction date to filter only those dailytable records whose timestamp < next transaction date
                            //while ((dailyRowNum < dailyTable.Rows.Count) &&
                            //        ((currentPriceDate = System.Convert.ToDateTime(dailyTable.Rows[dailyRowNum]["TIMESTAMP"].ToString())) <= nextTxnDate))
                            while (dailyRowNum < dailyTable.Rows.Count)
                            {
                                currentPriceDate = System.Convert.ToDateTime(dailyTable.Rows[dailyRowNum]["TIMESTAMP"].ToString());
                                //we need to identify if current transaction as the last one

                                if (((blasttxn) && (currentPriceDate <= nextTxnDate)) ||
                                        ((!blasttxn) && (currentPriceDate < nextTxnDate)))
                                {
                                    currentPrice = System.Convert.ToDouble(string.Format("{0:0.00}", dailyTable.Rows[dailyRowNum]["CLOSE"]));

                                    cumValue = Math.Round((currentPrice * cumQty), 2);
                                    currentValue = Math.Round((currentPrice * purchaseQty), 2);

                                    try
                                    {
                                        yearsInvested = Math.Round(((currentPriceDate.Date - currentTxnDate.Date).TotalDays) / 365.25, 2); // 4);
                                        totalYearsInvested = Math.Round(((currentPriceDate.Date - firstPurchaseDate.Date).TotalDays) / 365.25, 2); // 4);
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("PortfolioValuation: " + ex.Message);
                                        yearsInvested = Math.Round(0.00, 2);
                                        totalYearsInvested = Math.Round(0.00, 2);
                                    }


                                    try
                                    {
                                        arr = Math.Round(0.00, 2);
                                        if (Math.Round(yearsInvested, 0) != 0)
                                        {
                                            arr = Math.Round(Math.Pow((currentValue / investmentCost), (1 / yearsInvested)) - 1, 2);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("openMFPortfolio: " + ex.Message);
                                        arr = Math.Round(0.00, 2);
                                    }

                                    try
                                    {
                                        totalARR = Math.Round(0.00, 2);
                                        if (Math.Round(totalYearsInvested, 0) != 0)
                                        {
                                            totalARR = Math.Round(Math.Pow((cumValue / cumCost), (1 / totalYearsInvested)) - 1, 2);
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine("openMFPortfolio: " + ex.Message);
                                        totalARR = Math.Round(0.00, 2);
                                    }

                                    //following will come from current portfolio transaction as is
                                    if (interval.Equals("1d"))
                                    {
                                        dailyTable.Rows[dailyRowNum]["TIMESTAMP"] = System.Convert.ToDateTime(dailyTable.Rows[dailyRowNum]["TIMESTAMP"]).ToString("yyyy-MM-dd");
                                    }
                                    else
                                    {
                                        dailyTable.Rows[dailyRowNum]["TIMESTAMP"] = System.Convert.ToDateTime(dailyTable.Rows[dailyRowNum]["TIMESTAMP"]).ToString("yyyy-MM-dd") + " 17:00:00";
                                    }
                                    dailyTable.Rows[dailyRowNum]["ID"] = portfolioTransactionTable.Rows[txnRowNum]["ID"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["MASTERID"] = portfolioTransactionTable.Rows[txnRowNum]["MASTERID"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["STOCKID"] = portfolioTransactionTable.Rows[txnRowNum]["STOCKID"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["COMP_NAME"] = portfolioTransactionTable.Rows[txnRowNum]["COMP_NAME"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["SERIES"] = portfolioTransactionTable.Rows[txnRowNum]["SERIES"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["PURCHASE_DATE"] = currentTxnDate.ToString("dd-MM-yyyy"); //portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_DATE"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["PURCHASE_PRICE"] = purchasePrice; //portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_PRICE"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["PURCHASE_QTY"] = purchaseQty; //portfolioTransactionTable.Rows[txnRowNum]["PURCHASE_QTY"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["COMMISSION_TAXES"] = purchaseCommission; //portfolioTransactionTable.Rows[txnRowNum]["COMMISSION_TAXES"].ToString(); //Portfolio query
                                    dailyTable.Rows[dailyRowNum]["INVESTMENT_COST"] = investmentCost; //portfolioTransactionTable.Rows[txnRowNum]["INVESTMENT_COST"].ToString(); //Portfolio query

                                    //following are the calculated value using the current stock price and current date from daily table row
                                    dailyTable.Rows[dailyRowNum]["CURRENTVALUE"] = currentValue;
                                    dailyTable.Rows[dailyRowNum]["YearsInvested"] = yearsInvested;
                                    dailyTable.Rows[dailyRowNum]["ARR"] = arr;
                                    dailyTable.Rows[dailyRowNum]["CumulativeQty"] = cumQty;
                                    dailyTable.Rows[dailyRowNum]["CumulativeCost"] = cumCost;
                                    dailyTable.Rows[dailyRowNum]["CumulativeValue"] = cumValue;
                                    dailyTable.Rows[dailyRowNum]["FirstPurchaseDate"] = firstPurchaseDate.ToString("dd-MM-yyyy");
                                    dailyTable.Rows[dailyRowNum]["CumulativeYearsInvested"] = totalYearsInvested;
                                    dailyTable.Rows[dailyRowNum]["CumulativeARR"] = totalARR;
                                    //dailyTable.Rows[dailyRowNum]["CurrentValue"] = currentValue;
                                    dailyTable.Rows[dailyRowNum]["PORTFOLIO_FLAG"] = bPortfolioFlag.ToString();
                                    if (bPortfolioFlag)
                                    {
                                        bPortfolioFlag = false;
                                    }
                                    dailyRowNum++;
                                }
                                else
                                    break;
                            }
                            //now we have added one transaction from portfolio data to daily records table for current symbol from portfolio table
                        }
                        //now we have added all transactions from portfolio data including cumulative values in the daily records table for current table
                        //now merge the nav records table in valuation table

                        if (valuationTable == null)
                        {
                            valuationTable = dailyTable.Clone();
                        }
                        valuationTable.Merge(dailyTable, true, MissingSchemaAction.Ignore);

                        //now clear the data tables;
                        dailyTable.Rows.Clear();
                        dailyTable.Clear();
                        dailyTable.Dispose();
                        dailyTable = null;

                        portfolioTransactionTable.Rows.Clear();
                        portfolioTransactionTable.Clear();
                        portfolioTransactionTable.Dispose();
                        portfolioTransactionTable = null;
                    } //we have added transaction for one symbol to daily table
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

            double currentPrice = 0.00, cumQty = 0.00, cumCost = 0.00, cumValue = 0.00, investmentCost = 0.00, currentValue = 0.00, yearsInvested = 0.00, arr = 0.00;
            double totalYearsInvested = 0.00, totalARR = 0.00;

            DateTime currentDate, purchaseDate, firstPurchaseDate, lastPurchaseDate;
            currentDate = DateTime.Today;
            firstPurchaseDate = DateTime.MinValue;

            //first check if this is transaction for new symbol or for same symbol. Depending that currentPrice and CurrentDate will be assigned
            DataTable sourceTable = (DataTable)sender;
            int rowIndex = sourceTable.Rows.Count;
            rowIndex--; //this is the current row represented by e
            if (sourceTable.Rows.Count > 1)
            {
                rowIndex--; //this is the row prior to e
            }

            //DataTable quoteTable = GetQuote(e.Row["SYMBOL"].ToString() + "." + e.Row["EXCHANGE"].ToString());
            //if (quoteTable != null)
            //{
            //    currentPrice = System.Convert.ToDouble(string.Format("{0:0.00}", quoteTable.Rows[0]["price"].ToString()));
            //    currentDate = System.Convert.ToDateTime(quoteTable.Rows[0]["latestDay"].ToString());
            //}

            //if this is the first row or if the current row symbol is different than previous row symbol - fetch quote from market
            if ((rowIndex == 0) || (e.Row["SYMBOL"].ToString().Equals(sourceTable.Rows[rowIndex]["SYMBOL"].ToString()) == false))
            {
                cumQty = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["PURCHASE_QTY"].ToString()));
                cumCost = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["INVESTMENT_COST"].ToString()));
                firstPurchaseDate = System.Convert.ToDateTime(e.Row["PURCHASE_DATE"].ToString());
                lastPurchaseDate = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["PURCHASE_DATE"].ToString());

                //DataTable quoteTable = GetQuote(e.Row["SYMBOL"].ToString() + "." + e.Row["EXCHANGE"].ToString());
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

                cumQty = System.Convert.ToDouble(string.Format("{0:0.00}", sourceTable.Rows[rowIndex]["CumulativeQty"].ToString())) +
                        System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["PURCHASE_QTY"].ToString()));
                cumCost = System.Convert.ToDouble(string.Format("{0:0.00}", sourceTable.Rows[rowIndex]["CumulativeCost"].ToString())) +
                        System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["INVESTMENT_COST"].ToString()));

                firstPurchaseDate = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["FirstPurchaseDate"].ToString());
                lastPurchaseDate = System.Convert.ToDateTime(sourceTable.Rows[rowIndex]["LastPurchaseDate"].ToString());
            }

            cumValue = Math.Round((cumQty * currentPrice), 4);

            currentValue = Math.Round(currentPrice * System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["PURCHASE_QTY"])), 4);
            purchaseDate = System.Convert.ToDateTime(e.Row["PURCHASE_DATE"].ToString());

            try
            {
                yearsInvested = Math.Round(((currentDate.Date - purchaseDate.Date).TotalDays) / 365.25, 2); // 4);
                totalYearsInvested = Math.Round(((currentDate.Date - firstPurchaseDate.Date).TotalDays) / 365.25, 2); // 4);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                yearsInvested = Math.Round(0.00, 4);
            }
            investmentCost = System.Convert.ToDouble(string.Format("{0:0.00}", e.Row["INVESTMENT_COST"]));

            try
            {
                arr = Math.Round(0.00, 4);
                if (yearsInvested != 0)
                {
                    arr = Math.Round(Math.Pow((currentValue / investmentCost), (1 / yearsInvested)) - 1, 4);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                arr = Math.Round(0.00, 4);
            }
            try
            {
                totalARR = Math.Round(0.00, 4);
                if (totalYearsInvested != 0)
                {
                    totalARR = Math.Round(Math.Pow((cumValue / cumCost), (1 / totalYearsInvested)) - 1, 4);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                totalARR = Math.Round(0.00, 4);
            }
            //e.Row["PurchaseDate"] = purchasePrice.ToString("yyyy-MM-dd");
            //e.Row["PurchaseNAV"] = string.Format("{0:0.0000}", e.Row["PurchaseNAV"]);
            //e.Row["PurchaseUnits"] = string.Format("{0:0.0000}", e.Row["PurchaseUnits"]);
            //e.Row["ValueAtCost"] = string.Format("{0:0.0000}", valueAtCost);

            e.Row["CURRENTPRICE"] = string.Format("{0:0.00}", currentPrice);
            //e.Row["CURRENTDATE"] = currentDate.ToString("yyyy-MM-dd hh:mm:ss");
            //e.Row["CURRENTDATE"] = currentDate.ToString("yyyy-MM-dd");
            e.Row["CURRENTDATE"] = currentDate.ToString("dd-MM-yyyy");
            e.Row["CURRENTVALUE"] = string.Format("{0:0.00}", currentValue);
            //e.Row["YearsInvested"] = string.Format("{0:0.0000}", yearsInvested);
            e.Row["YearsInvested"] = string.Format("{0:0.00}", yearsInvested);
            try
            {
                //e.Row["ARR"] = string.Format("{0:0.0000}", arr);
                e.Row["ARR"] = string.Format("{0:0.00}", arr);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                //e.Row["ARR"] = string.Format("{0:0.0000}", 0);
                e.Row["ARR"] = string.Format("{0:0.00}", 0);
            }

            e.Row["CumulativeQty"] = string.Format("{0:0.00}", cumQty);
            e.Row["CumulativeCost"] = string.Format("{0:0.00}", cumCost);
            e.Row["CumulativeValue"] = string.Format("{0:0.00}", cumValue);
            //e.Row["FirstPurchaseDate"] = firstPurchaseDate.ToString("yyyy-MM-dd hh:mm:ss");
            //e.Row["FirstPurchaseDate"] = firstPurchaseDate.ToString("yyyy-MM-dd");
            e.Row["FirstPurchaseDate"] = firstPurchaseDate.ToString("dd-MM-yyyy");
            //e.Row["LastPurchaseDate"] = lastPurchaseDate.ToString("yyyy-MM-dd");
            //e.Row["LastPurchaseDate"] = lastPurchaseDate.ToString("yyyy-MM-dd");
            e.Row["LastPurchaseDate"] = lastPurchaseDate.ToString("dd-MM-yyyy");
            //e.Row["CumulativeYearsInvested"] = string.Format("{0:0.0000}", totalYearsInvested);
            e.Row["CumulativeYearsInvested"] = string.Format("{0:0.00}", totalYearsInvested);
            try
            {
                //e.Row["CumulativeARR"] = string.Format("{0:0.0000}", totalARR);
                e.Row["CumulativeARR"] = string.Format("{0:0.00}", totalARR);
            }
            catch (Exception ex)
            {
                Console.WriteLine("openMFPortfolio: " + ex.Message);
                //e.Row["CumulativeARR"] = string.Format("{0:0.0000}", 0);
                e.Row["CumulativeARR"] = string.Format("{0:0.00}", 0);
            }

            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerStockPortfolioTableRowChanged);
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
                //"STOCKMASTER.EXCHANGE, (STOCKMASTER.SYMBOL || '.' || STOCKMASTER.EXCHANGE) as SYMBOL, STOCKMASTER.COMP_NAME, STOCKMASTER.SERIES, " +
                "STOCKMASTER.EXCHANGE, STOCKMASTER.SYMBOL, STOCKMASTER.COMP_NAME, STOCKMASTER.SERIES, " +
                //"strftime('%Y-%m-%d', STOCKPORTFOLIO.PURCHASE_DATE) AS PURCHASE_DATE, STOCKPORTFOLIO.PURCHASE_PRICE, STOCKPORTFOLIO.PURCHASE_QTY, " +
                "strftime('%d-%m-%Y', STOCKPORTFOLIO.PURCHASE_DATE) AS PURCHASE_DATE, STOCKPORTFOLIO.PURCHASE_PRICE, STOCKPORTFOLIO.PURCHASE_QTY, " +
                "STOCKPORTFOLIO.COMMISSION_TAXES, STOCKPORTFOLIO.INVESTMENT_COST FROM STOCKMASTER " +
                "INNER JOIN STOCKPORTFOLIO ON STOCKPORTFOLIO.STOCKMASTER_ROWID = STOCKMASTER.ROWID " +
                "WHERE STOCKPORTFOLIO.MASTER_ROWID = " + stockportfolioMasterRowId + " " +
                "ORDER BY STOCKPORTFOLIO.STOCKMASTER_ROWID ASC, STOCKPORTFOLIO.PURCHASE_DATE ASC";

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

                    //resultDataTable.Columns.Add("PURCHASE_DATE", typeof(DateTime)); //PurchaseDate
                    resultDataTable.Columns.Add("PURCHASE_DATE", typeof(string)); //PurchaseDate

                    resultDataTable.Columns.Add("PURCHASE_PRICE", typeof(decimal)); //PurchaseNAV
                    resultDataTable.Columns.Add("PURCHASE_QTY", typeof(decimal)); //PurchaseUnits
                    resultDataTable.Columns.Add("COMMISSION_TAXES", typeof(decimal)); //ValueAtCost
                    resultDataTable.Columns.Add("INVESTMENT_COST", typeof(decimal)); //ValueAtCost

                    //resultDataTable.Columns.Add("CURRENTDATE", typeof(DateTime)); //PurchaseDate
                    resultDataTable.Columns.Add("CURRENTDATE", typeof(string)); //PurchaseDate
                    resultDataTable.Columns.Add("CURRENTPRICE", typeof(decimal));
                    resultDataTable.Columns.Add("CURRENTVALUE", typeof(decimal));
                    resultDataTable.Columns.Add("YearsInvested", typeof(decimal));
                    resultDataTable.Columns.Add("ARR", typeof(decimal));

                    resultDataTable.Columns.Add("CumulativeQty", typeof(decimal));
                    resultDataTable.Columns.Add("CumulativeCost", typeof(decimal));
                    resultDataTable.Columns.Add("CumulativeValue", typeof(decimal));
                    //resultDataTable.Columns.Add("FirstPurchaseDate", typeof(DateTime));
                    resultDataTable.Columns.Add("FirstPurchaseDate", typeof(string));
                    resultDataTable.Columns.Add("CumulativeYearsInvested", typeof(decimal));
                    resultDataTable.Columns.Add("CumulativeARR", typeof(decimal));
                    //resultDataTable.Columns.Add("LastPurchaseDate", typeof(DateTime));
                    resultDataTable.Columns.Add("LastPurchaseDate", typeof(string));

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
        /// Method to get for supplied userid all portfolio name, total cost, total valuation
        /// if will get all portfolios for a user, 
        /// then for each portfolio id it will get the portfolio table
        ///     for each symbol in current portfolio table it will add to cumulative cost & find valuation & add it to cumulative valuation
        /// </summary>
        /// <param name="userid"></param>
        /// <returns>DataTable with data or empty datatable</returns>
        public DataTable getAllPortfolioTableForUserId(string userid)
        {
            DataTable resultDataTable = null;
            double dblCost = 0.00;
            double dblValue = 0.00;
            double dblCumCost = 0.00;
            double dblCumValue = 0.00;

            try
            {
                //get all records from portfolio table matching portfolioname
                //get related scheme name, scheme id from schemes table
                //get latest NAV from NAVrecords table where NAVDate = schemes.todate
                resultDataTable = new DataTable();
                //FundHouse;FundName;SCHEME_CODE;PurchaseDate;PurchaseNAV;PurchaseUnits;ValueAtCost
                resultDataTable.Columns.Add("ROWID", typeof(long)); //FundHouse
                resultDataTable.Columns.Add("PORTFOLIO_NAME", typeof(string)); //FundHouse
                resultDataTable.Columns.Add("CumulativeCost", typeof(decimal));
                resultDataTable.Columns.Add("CumulativeValue", typeof(decimal));

                //fist get all portfolios for the userid
                DataTable tablePortfolioMaster = getPortfolioMaster(userid);
                if ((tablePortfolioMaster != null) && (tablePortfolioMaster.Rows.Count > 0))
                {
                    //for each portfolio in the master table get portfolio table
                    foreach (DataRow rowMaster in tablePortfolioMaster.Rows)
                    {
                        dblCost = 0.00;
                        dblValue = 0.00;
                        dblCumCost = 0.00;
                        dblCumValue = 0.00;

                        DataTable tablePortfolio = getStockPortfolioTable(rowMaster["ROWID"].ToString());
                        //we have specific portfolio table. loop through the table and cum up the cost & value
                        if ((tablePortfolio != null) && (tablePortfolio.Rows.Count > 0))
                        {
                            foreach (DataRow rowPortfolio in tablePortfolio.Rows)
                            {
                                dblCost = Convert.ToDouble(rowPortfolio["INVESTMENT_COST"].ToString());
                                dblValue = Convert.ToDouble(rowPortfolio["CURRENTVALUE"].ToString());

                                dblCumCost += dblCost;
                                dblCumValue += dblValue;
                            }
                        }
                        resultDataTable.Rows.Add(new object[] {
                                                                    rowMaster["ROWID"],
                                                                    rowMaster["PORTFOLIO_NAME"],
                                                                    Math.Round(dblCumCost, 2),
                                                                    Math.Round(dblCumValue, 2)
                                                                });
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getAllPortfolioTableForUserId: " + ex.Message);
            }

            return resultDataTable;
        }
        #endregion

        #region import CSV
        public DataTable readSourceCSV(StreamReader reader, bool columnHeader = true, char separatorChar = ',')
        {
            DataTable returnDT = null;
            try
            {
                if (reader != null)
                {
                    string record = reader.ReadLine();
                    string filteredRecord;
                    //string filteredRecord = new string(record.Where(c => (char.IsLetterOrDigit(c))).ToArray());

                    //time,Real Lower Band,Real Middle Band,Real Upper Band

                    returnDT = new DataTable();

                    string[] fields;

                    returnDT.RowChanged += new DataRowChangeEventHandler(handlerforCSVSourceNewRow);

                    if (columnHeader == true)
                    {
                        //data has column names as first row
                        fields = record.Split(separatorChar);

                        for (int i = 0; i < fields.Length; i++)
                        {

                            //myString.All(c => char.IsLetterOrDigit(c)); NOT USED here

                            //using Where to remove unwanted characters other than normal alph numbneric. The function uses predicate to evaluate condition for current char
                            //and then fiters those characters. Here we will use IsLetterOrDigit. Which means all characters in the source string which are not Letter or Digit
                            //will be removed
                            filteredRecord = new string(fields[i].Where(c => (char.IsLetterOrDigit(c) || char.IsWhiteSpace(c))).ToArray());
                            returnDT.Columns.Add(filteredRecord.Trim(), typeof(string));
                        }
                    }
                    else
                    {
                        //there is no column names row. just create temp column names in table 
                        //using count along with predicate to check for separator count
                        var count = record.Count(x => x == separatorChar);
                        for (int i = 1; i <= count; i++)
                        {
                            returnDT.Columns.Add("Col_" + i, typeof(string));
                        }
                        //since we have already read the first row which is actual data we need to add this in the table
                        fields = record.Split(separatorChar);
                        returnDT.Rows.Add(fields);
                    }

                    //now add rest of the data
                    while (!reader.EndOfStream)
                    {
                        record = reader.ReadLine();
                        //filteredRecord = new string(record.Where(c => (char.IsLetterOrDigit(c))).ToArray());
                        fields = record.Split(separatorChar);
                        returnDT.Rows.Add(fields);
                    }
                    returnDT.RowChanged -= new DataRowChangeEventHandler(handlerforCSVSourceNewRow);

                }
            }
            catch
            {
                if (returnDT != null)
                {
                    returnDT.Clear();
                    returnDT.Dispose();
                }
                returnDT = null;
            }
            return returnDT;
        }

        private void handlerforCSVSourceNewRow(object sender, DataRowChangeEventArgs e)
        {
            e.Row.Table.RowChanged -= new DataRowChangeEventHandler(handlerforCSVSourceNewRow); ;

            for (int i = 0; i < e.Row.Table.Columns.Count; i++)
            {
                //For each column content we have to check & remove any unwanted characters
                //using Where to remove unwanted characters other than normal alph numbneric. The function uses predicate to evaluate condition for current char
                //and then fiters those characters. Here we will use IsLetterOrDigit. Which means all characters in the source string which are not Letter or Digit
                //will be removed
                e.Row[i] = new string(e.Row[i].ToString().Where(c => (char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || char.IsPunctuation(c))).ToArray());
            }
            e.Row.Table.RowChanged += new DataRowChangeEventHandler(handlerforCSVSourceNewRow); ;
        }
        #endregion
        #region import excel

        public DataTable ReadExcelFile(string filePath, string sheetName, bool bHasColumnNames)
        {
            DataTable dt = new DataTable();
            try
            {
                // Open the Excel file using ClosedXML.
                // Keep in mind the Excel file cannot be open when trying to read it
                using (XLWorkbook workBook = new XLWorkbook(filePath))
                {
                    //Read the first Sheet from Excel file.
                    IXLWorksheet workSheet = workBook.Worksheet(sheetName);

                    //Create a new DataTable.

                    //Loop through the Worksheet rows.
                    bool firstRow = true;
                    int i = 1, j = 0;
                    DataRow newImportedRow;
                    IXLRow titleRow = workSheet.Rows().First<IXLRow>();

                    foreach (IXLCell cell in titleRow.Cells())
                    {
                        if (bHasColumnNames)
                        {
                            dt.Columns.Add(cell.Value.ToString(), typeof(string));
                        }
                        else
                        {
                            dt.Columns.Add("COLUMN_" + i.ToString(), typeof(string));
                            i++;
                        }
                    }

                    foreach (IXLRow row in workSheet.Rows())
                    {
                        //Use the first row to add columns to DataTable.
                        if ((firstRow) && (bHasColumnNames))
                        {
                            firstRow = false;
                            continue;
                        }
                        else
                        {
                            //Add rows to DataTable.
                            newImportedRow = dt.NewRow();
                            j = 0;
                            //foreach (IXLCell cell in row.Cells(row.FirstCellUsed().Address.ColumnNumber, row.LastCellUsed().Address.ColumnNumber))
                            foreach (IXLCell cell in row.Cells())
                            {
                                newImportedRow[j] = new string(cell.Value.ToString().Where(c => (char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || char.IsPunctuation(c))).ToArray());
                                j++;
                            }
                            dt.Rows.Add(newImportedRow);
                        }
                    }
                }
                File.Delete(filePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                if(dt != null)
                {
                    dt.Clear();
                    dt.Dispose();
                }
                dt = null;
            }
            return dt;
        }

        //following is the original method for ClosedXML
        //public static DataTable ImportExceltoDatatable(string filePath, string sheetName)
        //{
        //    // Open the Excel file using ClosedXML.
        //    // Keep in mind the Excel file cannot be open when trying to read it
        //    using (XLWorkbook workBook = new XLWorkbook(filePath))
        //    {
        //        //Read the first Sheet from Excel file.
        //        IXLWorksheet workSheet = workBook.Worksheet(1);

        //        //Create a new DataTable.
        //        DataTable dt = new DataTable();

        //        //Loop through the Worksheet rows.
        //        bool firstRow = true;
        //        foreach (IXLRow row in workSheet.Rows())
        //        {
        //            //Use the first row to add columns to DataTable.
        //            if (firstRow)
        //            {
        //                foreach (IXLCell cell in row.Cells())
        //                {
        //                    dt.Columns.Add(cell.Value.ToString());
        //                }
        //                firstRow = false;
        //            }
        //            else
        //            {
        //                //Add rows to DataTable.
        //                dt.Rows.Add();
        //                int i = 0;

        //                foreach (IXLCell cell in row.Cells(row.FirstCellUsed().Address.ColumnNumber, row.LastCellUsed().Address.ColumnNumber))
        //                {
        //                    dt.Rows[dt.Rows.Count - 1][i] = cell.Value.ToString();
        //                    i++;
        //                }
        //            }
        //        }

        //        return dt;
        //    }
        //}
        //public DataTable ReadSourceExcelFile(string filename, bool bHasColumnNames, string nameWorksheet)
        //{
        //    DataTable ContentTable = null;
        //    try
        //    {
        //        string connectionString = string.Empty;

        //        switch (Path.GetExtension(filename).ToUpperInvariant())
        //        {
        //            case ".XLS":
        //                connectionString = string.Format("Provider=Microsoft.Jet.OLEDB.4.0; Data Source={0}; Extended Properties=Excel 8.0;", filename);
        //                break;

        //            case ".XLSX":
        //                connectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0}; Extended Properties=Excel 12.0;", filename);
        //                break;
        //            default:
        //                throw (new Exception("File extension is missing"));
        //        }

        //        //if (extrn == ".xls")
        //        //    //Connectionstring for excel v8.0    

        //        //    connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filename + ";Extended Properties=\"Excel 8.0;HDR=Yes;IMEX=1\"";
        //        //else
        //        //    //Connectionstring fo excel v12.0    
        //        //    connString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filename + ";Extended Properties=\"Excel 12.0;HDR=Yes;IMEX=1\"";

        //        OleDbConnection OledbConn = new OleDbConnection(connectionString);

        //        OleDbCommand OledbCmd = new OleDbCommand();
        //        OledbCmd.Connection = OledbConn;
        //        OledbConn.Open();
        //        //OledbCmd.CommandText = "Select * from [StudentDetails$]";
        //        OledbCmd.CommandText = "Select * from [" + nameWorksheet + "$]";
        //        OleDbDataReader dr = OledbCmd.ExecuteReader();
        //        if (dr.HasRows)
        //        {
        //            ContentTable = new DataTable();

        //            //read first line
        //            dr.Read();

        //            if (bHasColumnNames)
        //            {
        //                //if first row in file has rows then use the cell content to create columns
        //                for (int i = 0; i < dr.FieldCount; i++)
        //                {
        //                    ContentTable.Columns.Add(dr[i].ToString().Trim(), typeof(string));
        //                }
        //                //in this case we will have to read the next data row
        //                dr.Read();
        //            }
        //            else
        //            {
        //                //since first row does not have column names, we will create adhoc column names for each column
        //                for (int i = 0; i < dr.FieldCount; i++)
        //                {
        //                    ContentTable.Columns.Add("COLUMN_" + i.ToString(), typeof(string));
        //                }
        //            }
        //            DataRow newRow;
        //            do
        //            {
        //                newRow = ContentTable.NewRow();
        //                for (int i = 0; i < dr.FieldCount; i++)
        //                {
        //                    //this predicate only letter or digit or whitespace or punctuation marks and removes any unwanted characters
        //                    newRow[i] = new string(dr[i].ToString().Where(c => (char.IsLetterOrDigit(c) || char.IsWhiteSpace(c) || char.IsPunctuation(c))).ToArray());
        //                }
        //                ContentTable.Rows.Add(newRow);

        //            } while (dr.Read());
        //        }
        //        dr.Close();

        //        OledbConn.Close();
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine(ex.Message);
        //        if (ContentTable != null)
        //        {
        //            ContentTable.Clear();
        //            ContentTable.Dispose();
        //        }
        //        ContentTable = null;
        //    }
        //    return ContentTable;
        //}

        /// <summary>
        /// We will not use this method as it required office interop. Method reads & returns comma separated worksheet names
        /// </summary>
        /// <param name="excelFilePath"></param>
        /// <returns></returns>
        //public string GetWorksheetsNames(string excelFilePath)
        //{
        //    Microsoft.Office.Interop.Excel.Application xlApp = new Microsoft.Office.Interop.Excel.Application();
        //    Microsoft.Office.Interop.Excel.Workbook excelBook = xlApp.Workbooks.Open("D:\\Book1.xlsx");

        //    string excelSheets = string.Empty;
        //    int i = 0;
        //    foreach (Microsoft.Office.Interop.Excel.Worksheet wSheet in excelBook.Worksheets)
        //    {
        //        excelSheets[i] = wSheet.Name;
        //        i++;
        //    }
        //}

        //public DataSet ReadExcel(string excelFilePath, string workSheetName)
        //{
        //    DataSet dsWorkbook = new DataSet();

        //    string connectionString = string.Empty;

        //    switch (Path.GetExtension(excelFilePath).ToUpperInvariant())
        //    {
        //        case ".XLS":
        //            connectionString = string.Format("Provider=Microsoft.Jet.OLEDB.4.0; Data Source={0}; Extended Properties=Excel 8.0;", excelFilePath);
        //            break;

        //        case ".XLSX":
        //            connectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0}; Extended Properties=Excel 12.0;", excelFilePath);
        //            break;

        //    }

        //    if (!String.IsNullOrEmpty(connectionString))
        //    {
        //        //MyCommand = new System.Data.OleDb.OleDbDataAdapter("select * from [Sheet1$]", MyConnection);
        //        string selectStatement = string.Format("SELECT * FROM [{0}$]", workSheetName);

        //        using (OleDbDataAdapter adapter = new OleDbDataAdapter(selectStatement, connectionString))
        //        {
        //            adapter.Fill(dsWorkbook, workSheetName);
        //        }
        //    }

        //    return dsWorkbook;
        //}


        #endregion
    }
}
