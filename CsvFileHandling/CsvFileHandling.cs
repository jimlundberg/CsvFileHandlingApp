using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using ReadWriteCsv;

namespace ReadWriteCsv
{
    /// <summary>
    /// Class to store one CSV row
    /// </summary>
    public class CsvRow : List<string>
    {
        public string LineText { get; set; }
    }

    /// <summary>
    /// Class to write data to a CSV file
    /// </summary>
    public class CsvFileWriter : StreamWriter
    {
        public CsvFileWriter(Stream stream)
            : base(stream)
        {
        }

        public CsvFileWriter(string filename)
            : base(filename)
        {
        }

        /// <summary>
        /// Writes a single row to a CSV file.
        /// </summary>
        /// <param name="row">The row to be written</param>
        public void WriteRow(CsvRow row)
        {
            StringBuilder builder = new StringBuilder();
            bool firstColumn = true;
            foreach (string value in row)
            {
                // Add separator if this isn't the first value
                if (!firstColumn)
                    builder.Append(',');
                // Implement special handling for values that contain comma or quote
                // Enclose in quotes and double up any double quotes
                if (value.IndexOfAny(new char[] { '"', ',' }) != -1)
                    builder.AppendFormat("\"{0}\"", value.Replace("\"", "\"\""));
                else
                    builder.Append(value);
                firstColumn = false;
            }
            row.LineText = builder.ToString();
            WriteLine(row.LineText);
        }
    }

    /// <summary>
    /// Class to read data from a CSV file
    /// </summary>
    public class CsvFileReader : StreamReader
    {
        public CsvFileReader(Stream stream)
            : base(stream)
        {
        }

        public CsvFileReader(string filename)
            : base(filename)
        {
        }

        /// <summary>
        /// Reads a row of data from a CSV file
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ReadRow(CsvRow row)
        {
            row.LineText = ReadLine();
            if (String.IsNullOrEmpty(row.LineText))
                return false;

            int pos = 0;
            int rows = 0;

            while (pos < row.LineText.Length)
            {
                string value;

                // Special handling for quoted field
                if (row.LineText[pos] == '"')
                {
                    // Skip initial quote
                    pos++;

                    // Parse quoted value
                    int start = pos;
                    while (pos < row.LineText.Length)
                    {
                        // Test for quote character
                        if (row.LineText[pos] == '"')
                        {
                            // Found one
                            pos++;

                            // If two quotes together, keep one
                            // Otherwise, indicates end of value
                            if (pos >= row.LineText.Length || row.LineText[pos] != '"')
                            {
                                pos--;
                                break;
                            }
                        }
                        pos++;
                    }
                    value = row.LineText.Substring(start, pos - start);
                    value = value.Replace("\"\"", "\"");
                }
                else
                {
                    // Parse unquoted value
                    int start = pos;
                    while (pos < row.LineText.Length && row.LineText[pos] != ',')
                        pos++;
                    value = row.LineText.Substring(start, pos - start);
                }

                // Add field to list
                if (rows < row.Count)
                    row[rows] = value;
                else
                    row.Add(value);
                rows++;

                // Eat up to and including next comma
                while (pos < row.LineText.Length && row.LineText[pos] != ',')
                    pos++;
                if (pos < row.LineText.Length)
                    pos++;
            }
            // Delete any unused items
            while (row.Count > rows)
                row.RemoveAt(rows);

            // Return true if any columns read
            return (row.Count > 0);
        }
    }
}

namespace CsvFileHandling
{
    partial class CsvFileHandling
    {
        public class CsvReadWrite
        {
            public void WriteTest()
            {
                // Write sample data to CSV file
                using (CsvFileWriter writer = new CsvFileWriter("WriteTest.csv"))
                {
                    for (int i = 0; i < 100; i++)
                    {
                        CsvRow row = new CsvRow();
                        for (int j = 0; j < 5; j++)
                            row.Add(String.Format("Column{0}", j));
                        writer.WriteRow(row);
                    }
                }
            }

            public void ReadTest()
            {
                // Read sample data from CSV file
                using (CsvFileReader reader = new CsvFileReader("ReadTest.csv"))
                {
                    CsvRow row = new CsvRow();
                    while (reader.ReadRow(row))
                    {
                        foreach (string s in row)
                        {
                            Console.Write(s);
                            Console.Write(" ");
                        }
                        Console.WriteLine();
                    }
                }
            }
        }

        public class OperationsUtlity
        {
            public enum JobStatus
            {
                NONE,
                JOB_STARTED,
                EXECUTING,
                MONITORING_INPUT,
                COPYING_TO_PROCESSING,
                MONITORING_PROCESSING,
                MONITORING_TCPIP,
                COPYING_TO_ARCHIVE,
                COMPLETE
            }

            public DataTable createDataTable()
            {
                DataTable table = new DataTable();

                // Columns
                table.Columns.Add("Job", typeof(string));
                table.Columns.Add("Status", typeof(string));
                table.Columns.Add("TimeRecieved", typeof(DateTime));
                table.Columns.Add("TimeStarted", typeof(DateTime));
                table.Columns.Add("TimeCompleted", typeof(DateTime));

                // Data  
                table.Rows.Add("1185840_202003250942", JobStatus.JOB_STARTED, DateTime.Now, DateTime.MinValue, DateTime.MinValue);
                table.Rows.Add("1185840_202003250942", JobStatus.COPYING_TO_PROCESSING, DateTime.MinValue, DateTime.Now, DateTime.MinValue);
                table.Rows.Add("1185840_202003250942", JobStatus.EXECUTING, DateTime.MinValue, DateTime.Now, DateTime.MinValue);
                table.Rows.Add("1185840_202003250942", JobStatus.COPYING_TO_ARCHIVE, DateTime.MinValue, DateTime.Now, DateTime.MinValue);
                table.Rows.Add("1185840_202003250942", JobStatus.COMPLETE, DateTime.MinValue, DateTime.MinValue, DateTime.Now);

                return table;
            }
        }

        static void Main(string[] args)
        {
            OperationsUtlity util = new OperationsUtlity();
            DataTable data = util.createDataTable();
            Console.WriteLine(data);

            CsvReadWrite csv = new CsvReadWrite();
            csv.WriteTest();
            csv.ReadTest();

            Console.ReadKey();
        }
    }
}
