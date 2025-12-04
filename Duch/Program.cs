using System;
using System.IO;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql; // dla FbScript i FbBatchExecution
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        #region Entry point and commands
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "build-db":
                        {
                            string dbDir = GetArgValue(args, "--db-dir");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            BuildDatabase(dbDir, scriptsDir);
                            Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                            return 0;
                        }

                    case "export-scripts":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string outputDir = GetArgValue(args, "--output-dir");

                            ExportScripts(connStr, outputDir);
                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "update-db":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            UpdateDatabase(connStr, scriptsDir);
                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            // TODO:
            // 1) Utwórz pustą bazę danych FB 5.0 w katalogu databaseDirectory.
            // 2) Wczytaj i wykonaj kolejno skrypty z katalogu scriptsDirectory
            //    (tylko domeny, tabele, procedury).
            // 3) Obsłuż błędy i wyświetl raport.
            //throw new NotImplementedException();
            if (string.IsNullOrWhiteSpace(databaseDirectory))
                throw new ArgumentException("Parametr databaseDirectory jest wymagany.", nameof(databaseDirectory));

            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("Parametr scriptsDirectory jest wymagany.", nameof(scriptsDirectory));

            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Katalog ze skryptami nie istnieje: {scriptsDirectory}");

            // jeśli użytkownik podał już plik .fdb – używamy wprost;
            // jeśli podał tylko katalog – dokładamy domyślną nazwę
            string dbPath;
            if (Path.HasExtension(databaseDirectory))
            {
                dbPath = databaseDirectory;
                Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);
            }
            else
            {
                Directory.CreateDirectory(databaseDirectory);
                dbPath = Path.Combine(databaseDirectory, "database.fdb");
            }

            if (File.Exists(dbPath))
                File.Delete(dbPath);

            // 3) Tworzymy connection string i pustą bazę
            var csb = new FbConnectionStringBuilder
            {
                Database = dbPath,
                DataSource = "localhost",     // zakładamy lokalny serwer FB
                UserID = "SYSDBA",
                Password = "masterkey",
                Charset = "UTF8",
                Dialect = 3,
                Port = 3052
            };

            // Tworzymy bazę z typowymi parametrami (pageSize 8192)
            FbConnection.CreateDatabase(
                csb.ToString(),
                pageSize: 8192,
                forcedWrites: true,
                overwrite: true);

            // 4) Wykonujemy skrypty z katalogu scriptsDirectory
            var sqlFiles = Directory
                .EnumerateFiles(scriptsDirectory, "*.sql")
                .OrderBy(Path.GetFileName) // np. 01_domains.sql, 02_tables.sql, 03_procedures.sql
                .ToList();

            if (sqlFiles.Count == 0)
                throw new InvalidOperationException($"W katalogu {scriptsDirectory} nie znaleziono żadnych plików .sql.");

            var errors = new List<string>();

            using (var connection = new FbConnection(csb.ToString()))
            {
                connection.Open();

                foreach (var file in sqlFiles)
                {
                    var scriptText = File.ReadAllText(file);

                    if (string.IsNullOrWhiteSpace(scriptText))
                        continue;

                    try
                    {
                        var script = new FbScript(scriptText);
                        script.Parse();

                        var batch = new FbBatchExecution(connection);
                        batch.AppendSqlStatements(script);
                        batch.Execute();
                    }
                    catch (Exception ex)
                    {
                        errors.Add($"Plik '{Path.GetFileName(file)}': {ex.Message}");
                    }
                }
            }

            // 5) Raport z wykonania
            if (errors.Count > 0)
            {
                Console.WriteLine("Błędy podczas budowania bazy danych:");
                foreach (var err in errors)
                {
                    Console.WriteLine("  - " + err);
                }

                // sygnalizujemy błąd do Main(), żeby nie wypisał komunikatu o sukcesie
                throw new InvalidOperationException("Budowanie bazy zakończone z błędami. Szczegóły powyżej.");
            }

        }

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql / .json / .txt w outputDirectory.
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Parametr connectionString jest wymagany.", nameof(connectionString));

            if (string.IsNullOrWhiteSpace(outputDirectory))
                throw new ArgumentException("Parametr outputDirectory jest wymagany.", nameof(outputDirectory));

            Directory.CreateDirectory(outputDirectory);

            var domainNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            using (var connection = new FbConnection(connectionString))
            {
                connection.Open();

                string domainsSql = GenerateDomainsSql(connection, domainNames);
                string tablesSql = GenerateTablesSql(connection, domainNames);
                string proceduresSql = GenerateProceduresSql(connection);

                File.WriteAllText(Path.Combine(outputDirectory, "01_domains.sql"), domainsSql, Encoding.UTF8);
                File.WriteAllText(Path.Combine(outputDirectory, "02_tables.sql"), tablesSql, Encoding.UTF8);
                File.WriteAllText(Path.Combine(outputDirectory, "03_procedures.sql"), proceduresSql, Encoding.UTF8);
            }
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // TODO:
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.
            throw new NotImplementedException();
        }
        #endregion

        #region Export helpers
        private static string GenerateDomainsSql(FbConnection connection, HashSet<string> domainNames)
        {
            var sb = new StringBuilder();

            const string sql = @"
        SELECT
            TRIM(f.RDB$FIELD_NAME)           AS DOMAIN_NAME,
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            f.RDB$CHARACTER_LENGTH,
            cs.RDB$CHARACTER_SET_NAME,
            f.RDB$DEFAULT_SOURCE,
            f.RDB$NULL_FLAG,
            f.RDB$VALIDATION_SOURCE
        FROM RDB$FIELDS f
        LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
        WHERE (f.RDB$SYSTEM_FLAG IS NULL OR f.RDB$SYSTEM_FLAG = 0)
          AND f.RDB$FIELD_NAME NOT LIKE 'RDB$%'
        ORDER BY DOMAIN_NAME";

            using (var cmd = new FbCommand(sql, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var name = reader.GetString(reader.GetOrdinal("DOMAIN_NAME")).Trim();
                    if (string.IsNullOrEmpty(name))
                        continue;

                    domainNames.Add(name);

                    short fieldType = reader.GetInt16(reader.GetOrdinal("RDB$FIELD_TYPE"));
                    short? fieldSubType = GetNullableInt16(reader, "RDB$FIELD_SUB_TYPE");
                    short? fieldScale = GetNullableInt16(reader, "RDB$FIELD_SCALE");
                    short? fieldPrec = GetNullableInt16(reader, "RDB$FIELD_PRECISION");
                    int? charLen = GetNullableInt32(reader, "RDB$CHARACTER_LENGTH");
                    string charSet = GetNullableString(reader, "RDB$CHARACTER_SET_NAME");

                    string typeDef = GetSqlType(fieldType, fieldSubType, fieldScale, fieldPrec, charLen, charSet);

                    sb.Append("CREATE OR ALTER DOMAIN ")
                      .Append(name)
                      .Append(" AS ")
                      .Append(typeDef);

                    string defaultSource = GetNullableString(reader, "RDB$DEFAULT_SOURCE");
                    short? nullFlag = GetNullableInt16(reader, "RDB$NULL_FLAG");
                    string validation = GetNullableString(reader, "RDB$VALIDATION_SOURCE");

                    if (!string.IsNullOrWhiteSpace(defaultSource))
                        sb.Append(' ').Append(defaultSource.Trim());

                    if (nullFlag.HasValue && nullFlag.Value == 1)
                        sb.Append(" NOT NULL");

                    if (!string.IsNullOrWhiteSpace(validation))
                        sb.Append(' ').Append(validation.Trim());

                    sb.Append(';').AppendLine().AppendLine();
                }
            }

            return sb.ToString();
        }

        private static string GenerateTablesSql(FbConnection connection, HashSet<string> domainNames)
        {
            var sb = new StringBuilder();

            const string tablesSql = @"
        SELECT
            TRIM(r.RDB$RELATION_NAME) AS TABLE_NAME
        FROM RDB$RELATIONS r
        WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
          AND r.RDB$VIEW_BLR IS NULL
        ORDER BY TABLE_NAME";

            using (var cmdTables = new FbCommand(tablesSql, connection))
            using (var readerTables = cmdTables.ExecuteReader())
            {
                while (readerTables.Read())
                {
                    var tableName = readerTables.GetString(readerTables.GetOrdinal("TABLE_NAME")).Trim();
                    if (string.IsNullOrEmpty(tableName))
                        continue;

                    var columns = GetColumnsForTable(connection, tableName, domainNames);
                    if (columns.Count == 0)
                        continue;

                    sb.Append("CREATE TABLE ").Append(tableName).AppendLine(" (");

                    for (int i = 0; i < columns.Count; i++)
                    {
                        sb.Append("    ").Append(columns[i]);
                        if (i < columns.Count - 1)
                            sb.Append(',');
                        sb.AppendLine();
                    }

                    sb.Append(");").AppendLine().AppendLine();
                }
            }

            return sb.ToString();
        }

        private static List<string> GetColumnsForTable(FbConnection connection, string tableName, HashSet<string> domainNames)
        {
            const string sql = @"
        SELECT
            TRIM(rf.RDB$FIELD_NAME)      AS COLUMN_NAME,
            TRIM(f.RDB$FIELD_NAME)       AS FIELD_SOURCE,
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            f.RDB$CHARACTER_LENGTH,
            cs.RDB$CHARACTER_SET_NAME,
            rf.RDB$DEFAULT_SOURCE,
            rf.RDB$NULL_FLAG
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
        LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
        WHERE rf.RDB$RELATION_NAME = @TABLE_NAME
        ORDER BY rf.RDB$FIELD_POSITION";

            var result = new List<string>();

            using (var cmd = new FbCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@TABLE_NAME", tableName);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var colName = reader.GetString(reader.GetOrdinal("COLUMN_NAME")).Trim();
                        var fieldSrc = reader.GetString(reader.GetOrdinal("FIELD_SOURCE")).Trim();

                        short fieldType = reader.GetInt16(reader.GetOrdinal("RDB$FIELD_TYPE"));
                        short? fieldSubType = GetNullableInt16(reader, "RDB$FIELD_SUB_TYPE");
                        short? fieldScale = GetNullableInt16(reader, "RDB$FIELD_SCALE");
                        short? fieldPrec = GetNullableInt16(reader, "RDB$FIELD_PRECISION");
                        int? charLen = GetNullableInt32(reader, "RDB$CHARACTER_LENGTH");
                        string charSet = GetNullableString(reader, "RDB$CHARACTER_SET_NAME");

                        string typeDef;

                        if (domainNames.Contains(fieldSrc))
                        {
                            // kolumna oparta na domenie
                            typeDef = fieldSrc;
                        }
                        else
                        {
                            typeDef = GetSqlType(fieldType, fieldSubType, fieldScale, fieldPrec, charLen, charSet);
                        }

                        var colSb = new StringBuilder();
                        colSb.Append(colName).Append(' ').Append(typeDef);

                        string defaultSource = GetNullableString(reader, "RDB$DEFAULT_SOURCE");
                        short? nullFlag = GetNullableInt16(reader, "RDB$NULL_FLAG");

                        if (!string.IsNullOrWhiteSpace(defaultSource))
                            colSb.Append(' ').Append(defaultSource.Trim());

                        if (nullFlag.HasValue && nullFlag.Value == 1)
                            colSb.Append(" NOT NULL");

                        result.Add(colSb.ToString());
                    }
                }
            }

            return result;
        }

        private static string GenerateProceduresSql(FbConnection connection)
        {
            var sb = new StringBuilder();

            const string sql = @"
        SELECT
            TRIM(p.RDB$PROCEDURE_NAME) AS PROCEDURE_NAME,
            p.RDB$PROCEDURE_SOURCE     AS SOURCE
        FROM RDB$PROCEDURES p
        WHERE COALESCE(p.RDB$SYSTEM_FLAG, 0) = 0
        ORDER BY PROCEDURE_NAME";

            using (var cmd = new FbCommand(sql, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var procName = reader.GetString(reader.GetOrdinal("PROCEDURE_NAME")).Trim();
                    string body = GetNullableString(reader, "SOURCE") ?? string.Empty;

                    var inputs = new List<string>();
                    var outputs = new List<string>();
                    LoadProcedureParameters(connection, procName, inputs, outputs);

                    sb.Append("CREATE OR ALTER PROCEDURE ").Append(procName);

                    if (inputs.Count > 0)
                    {
                        sb.AppendLine().AppendLine("(");
                        for (int i = 0; i < inputs.Count; i++)
                        {
                            sb.Append("    ").Append(inputs[i]);
                            if (i < inputs.Count - 1)
                                sb.Append(',');
                            sb.AppendLine();
                        }
                        sb.AppendLine(")");
                    }
                    else
                    {
                        sb.AppendLine();
                    }

                    if (outputs.Count > 0)
                    {
                        sb.AppendLine("RETURNS (");
                        for (int i = 0; i < outputs.Count; i++)
                        {
                            sb.Append("    ").Append(outputs[i]);
                            if (i < outputs.Count - 1)
                                sb.Append(',');
                            sb.AppendLine();
                        }
                        sb.AppendLine(")");
                    }

                    var trimmedBody = body.TrimEnd();

                    if (string.IsNullOrWhiteSpace(trimmedBody))
                    {
                        sb.AppendLine("AS")
                          .AppendLine("BEGIN")
                          .AppendLine("END");
                    }
                    else
                    {
                        sb.AppendLine(trimmedBody);
                    }

                    if (!trimmedBody.EndsWith(";"))
                        sb.AppendLine(";");

                    sb.AppendLine();
                }
            }

            return sb.ToString();
        }

        private static void LoadProcedureParameters(
            FbConnection connection,
            string procedureName,
            List<string> inputs,
            List<string> outputs)
        {
            const string sql = @"
        SELECT
            TRIM(pp.RDB$PARAMETER_NAME) AS PARAM_NAME,
            pp.RDB$PARAMETER_TYPE,
            pp.RDB$PARAMETER_NUMBER,
            f.RDB$FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH,
            f.RDB$FIELD_PRECISION,
            f.RDB$FIELD_SCALE,
            f.RDB$CHARACTER_LENGTH,
            cs.RDB$CHARACTER_SET_NAME
        FROM RDB$PROCEDURE_PARAMETERS pp
        JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = pp.RDB$FIELD_SOURCE
        LEFT JOIN RDB$CHARACTER_SETS cs ON cs.RDB$CHARACTER_SET_ID = f.RDB$CHARACTER_SET_ID
        WHERE pp.RDB$PROCEDURE_NAME = @PROC_NAME
        ORDER BY pp.RDB$PARAMETER_TYPE, pp.RDB$PARAMETER_NUMBER";

            using (var cmd = new FbCommand(sql, connection))
            {
                cmd.Parameters.AddWithValue("@PROC_NAME", procedureName);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = reader.GetString(reader.GetOrdinal("PARAM_NAME")).Trim();
                        short paramType = reader.GetInt16(reader.GetOrdinal("RDB$PARAMETER_TYPE"));

                        short fieldType = reader.GetInt16(reader.GetOrdinal("RDB$FIELD_TYPE"));
                        short? fieldSubType = GetNullableInt16(reader, "RDB$FIELD_SUB_TYPE");
                        short? fieldScale = GetNullableInt16(reader, "RDB$FIELD_SCALE");
                        short? fieldPrec = GetNullableInt16(reader, "RDB$FIELD_PRECISION");
                        int? charLen = GetNullableInt32(reader, "RDB$CHARACTER_LENGTH");
                        string charSet = GetNullableString(reader, "RDB$CHARACTER_SET_NAME");

                        string typeDef = GetSqlType(fieldType, fieldSubType, fieldScale, fieldPrec, charLen, charSet);
                        string def = $"{name} {typeDef}";

                        if (paramType == 0)
                            inputs.Add(def);
                        else
                            outputs.Add(def);
                    }
                }
            }
        }
        #endregion

        #region Utils
        private static short? GetNullableInt16(FbDataReader reader, string columnName)
        {
            int ord = reader.GetOrdinal(columnName);
            return reader.IsDBNull(ord) ? (short?)null : reader.GetInt16(ord);
        }

        private static int? GetNullableInt32(FbDataReader reader, string columnName)
        {
            int ord = reader.GetOrdinal(columnName);
            return reader.IsDBNull(ord) ? (int?)null : reader.GetInt32(ord);
        }

        private static string? GetNullableString(FbDataReader reader, string columnName)
        {
            int ord = reader.GetOrdinal(columnName);
            return reader.IsDBNull(ord) ? null : reader.GetString(ord);
        }

        private static string GetSqlType(
            short fieldType,
            short? fieldSubType,
            short? fieldScale,
            short? fieldPrecision,
            int? charLen,
            string? charSet)
        {
            // bardzo uproszczone mapowanie wystarczające do typowych domen/tabel
            switch (fieldType)
            {
                case 7:   // SMALLINT / NUMERIC/DECIMAL
                case 8:   // INTEGER / NUMERIC/DECIMAL
                case 16:  // BIGINT / NUMERIC/DECIMAL
                    if (fieldSubType.HasValue && (fieldSubType == 1 || fieldSubType == 2))
                    {
                        int precision = fieldPrecision ?? 18;
                        int scale = fieldScale.HasValue ? -fieldScale.Value : 0;
                        string baseName = fieldSubType == 1 ? "NUMERIC" : "DECIMAL";

                        if (scale > 0)
                            return $"{baseName}({precision}, {scale})";
                        return $"{baseName}({precision})";
                    }

                    if (fieldType == 7) return "SMALLINT";
                    if (fieldType == 8) return "INTEGER";
                    if (fieldType == 16) return "BIGINT";
                    break;

                case 10: return "FLOAT";
                case 27: return "DOUBLE PRECISION";
                case 12: return "DATE";
                case 13: return "TIME";
                case 35: return "TIMESTAMP";

                case 14: // CHAR
                    {
                        int len = charLen ?? 1;
                        var sb = new StringBuilder($"CHAR({len})");
                        if (!string.IsNullOrWhiteSpace(charSet))
                            sb.Append(" CHARACTER SET ").Append(charSet.Trim());
                        return sb.ToString();
                    }

                case 37: // VARCHAR
                    {
                        int len = charLen ?? 1;
                        var sb = new StringBuilder($"VARCHAR({len})");
                        if (!string.IsNullOrWhiteSpace(charSet))
                            sb.Append(" CHARACTER SET ").Append(charSet.Trim());
                        return sb.ToString();
                    }

                case 261: // BLOB
                    if (fieldSubType == 1)
                        return "BLOB SUB_TYPE TEXT";
                    return "BLOB";
            }

            // awaryjnie
            return "BLOB";
        }

        #endregion
    }
}
