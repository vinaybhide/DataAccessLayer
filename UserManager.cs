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
    public class UserManager
    {
        static string dbFile = "mfdata.db";

        static public string GetDataFileFullName()
        {
            string sCurrentDir = AppDomain.CurrentDomain.BaseDirectory;
            string sFile = System.IO.Path.Combine(sCurrentDir, dbFile);
            //string sFilePath = Path.GetFullPath(sFile);
            string sFilePath = Path.GetPathRoot(sFile) + @"ProdDatabase\" + dbFile;

            return sFilePath;
        }

        /// <summary>
        /// metho that return rootfolder\ProdDatabase\
        /// </summary>
        /// <returns></returns>
        static public string GetDataFolder()
        {
            string sCurrentDir = AppDomain.CurrentDomain.BaseDirectory;
            string sFile = System.IO.Path.Combine(sCurrentDir, dbFile);
            //string sFilePath = Path.GetFullPath(sFile);
            string sDataFolder = Path.GetPathRoot(sFile) + @"ProdDatabase\";

            return sDataFolder;
        }

        public SQLiteConnection CreateConnection()
        {
            SQLiteConnection sqlite_conn = null;

            //string sCurrentDir = AppDomain.CurrentDomain.BaseDirectory;
            //string sFile = System.IO.Path.Combine(sCurrentDir, dbFile);
            ////string sFilePath = Path.GetFullPath(sFile);
            //string sFilePath = Path.GetPathRoot(sFile) + @"ProdDatabase\" + dbFile;

            string sFilePath = UserManager.GetDataFileFullName();

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

        /// <summary>
        /// Method to check if userid exists in USERMASTER or NOT
        /// </summary>
        /// <param name="userid">userid to check</param>
        /// <param name="sqlite_cmd"></param>
        /// <returns>ROWID of the matching userid else 0</returns>
        public long CheckUserExists(string userid, string password = null, SQLiteCommand sqlite_cmd = null)
        {
            long usermaster_rowid = 0;
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
                if (password == null)
                {
                    sqlite_cmd.CommandText = "SELECT ROWID FROM USERMASTER WHERE USERID = '" + userid + "'";
                }
                else
                {
                    sqlite_cmd.CommandText = "SELECT ROWID FROM USERMASTER WHERE USERID = '" + userid + "' AND PASSWORD = '" + password + "'";
                }
                try
                {
                    sqlite_datareader = sqlite_cmd.ExecuteReader();
                    if (sqlite_datareader.HasRows)
                    {
                        if (sqlite_datareader.Read())
                        {
                            //sqlite_datareader.Read();
                            usermaster_rowid = Int64.Parse(sqlite_datareader["ROWID"].ToString());
                        }
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("getPortfoliId: [" + userid + "]" + exSQL.Message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("getPortfoliId: [" + userid + "]" + ex.Message);
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
            return usermaster_rowid;
        }

        /// <summary>
        /// Method to register a user in the system. It will allow new user registration only if that userid does not exists in USERMASTER
        /// </summary>
        /// <param name="userid">user id to be registered</param>
        /// <param name="password">encrypted password</param>
        /// <returns>ROWID from USERMASTER for the row that was created else 0</returns>
        public long RegisterUser(string userid, string password)
        {
            long usermaster_id = 0;
            SQLiteConnection sqlite_conn = null;
            SQLiteCommand sqlite_cmd = null;

            try
            {
                sqlite_conn = CreateConnection();
                sqlite_cmd = sqlite_conn.CreateCommand();
                var transaction = sqlite_conn.BeginTransaction();
                try
                {
                    //first check if userid exists
                    if (CheckUserExists(userid, password, sqlite_cmd) <= 0)
                    {
                        sqlite_cmd.CommandText = "INSERT OR IGNORE INTO USERMASTER(USERID, PASSWORD) VALUES (@USERID, @PASSWORD)";
                        sqlite_cmd.Prepare();
                        sqlite_cmd.Parameters.AddWithValue("@USERID", userid);
                        sqlite_cmd.Parameters.AddWithValue("@PASSWORD", password);
                        if (sqlite_cmd.ExecuteNonQuery() > 0)
                        {
                            usermaster_id = CheckUserExists(userid, password, sqlite_cmd);
                        }
                    }
                }
                catch (SQLiteException exSQL)
                {
                    Console.WriteLine("CreateNewPortfolio: " + userid  + exSQL.Message);
                    usermaster_id = -1;
                }
                transaction.Commit();
                transaction.Dispose();
                transaction = null;
            }
            catch
            {
                usermaster_id = -1;
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
            return usermaster_id;
        }

    }
}
