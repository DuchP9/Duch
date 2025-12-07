using System;
using System.IO;
using FirebirdSql.Data.FirebirdClient;
using FirebirdSql.Data.Isql; // dla FbScript i FbBatchExecution
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Data.Common;
using System.IO; // dla StringReader

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\\db\\fb5" --scripts-dir "C:\\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\\scripts"
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
            if (string.IsNullOrWhiteSpace(databaseDirectory))
                throw new ArgumentException("Parametr databaseDirectory jest wymagany.", nameof(databaseDirectory));

            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("Parametr scriptsDirectory jest wymagany.", nameof(scriptsDirectory));

            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Katalog ze skryptami nie istnieje: {scriptsDirectory}");

            // ustalenie ścieżki do pliku bazy
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

            // jeśli istnieje – kasujemy
            if (File.Exists(dbPath))
                File.Delete(dbPath);

            // connection string – embedded + fbclient 32-bit
            var csb = new FbConnectionStringBuilder
            {
                Database = dbPath,
                UserID = "SYSDBA",
                Password = "masterkey",
                Charset = "UTF8",
                Dialect = 3,
                ServerType = FbServerType.Embedded,
                ClientLibrary = @"C:\Program Files (x86)\Firebird\Firebird_5_0_32\fbclient.dll"
            };

            // tworzenie pustej bazy
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

            if (errors.Count > 0)
            {
                Console.WriteLine("Błędy podczas budowania bazy danych:");
                foreach (var err in errors)
                {
                    Console.WriteLine("  - " + err);
                }

                throw new InvalidOperationException("Budowanie bazy zakończone z błędami. Szczegóły powyżej.");
            }
        }

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
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
                string proceduresSql = GenerateProceduresSql(connection, domainNames);

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
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new ArgumentException("Parametr connectionString jest wymagany.", nameof(connectionString));

            if (string.IsNullOrWhiteSpace(scriptsDirectory))
                throw new ArgumentException("Parametr scriptsDirectory jest wymagany.", nameof(scriptsDirectory));

            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Katalog ze skryptami nie istnieje: {scriptsDirectory}");

            var errors = new List<string>();

            // zakładamy standardowe nazwy plików, bo sam je tak generujesz
            string domainsPath = Path.Combine(scriptsDirectory, "01_domains.sql");
            string tablesPath = Path.Combine(scriptsDirectory, "02_tables.sql");
            string proceduresPath = Path.Combine(scriptsDirectory, "03_procedures.sql");

            string domainsScriptText = File.Exists(domainsPath) ? File.ReadAllText(domainsPath) : string.Empty;
            string tablesScriptText = File.Exists(tablesPath) ? File.ReadAllText(tablesPath) : string.Empty;
            string proceduresScriptText = File.Exists(proceduresPath) ? File.ReadAllText(proceduresPath) : string.Empty;

            // skrypty jako "źródło prawdy"
            var scriptDomains = ParseDomainCreateStatements(domainsScriptText);   // nazwa -> CREATE DOMAIN ...
            var scriptTables = ParseTableScripts(tablesScriptText);               // nazwa -> TableScriptInfo (CREATE TABLE + kolumny)

            using (var connection = new FbConnection(connectionString))
            {
                connection.Open();

                // ===== KROK 0: Zaczytaj stan bazy =====
                var dbDomainNames = GetUserDomainNames(connection);
                var dbTableNames = GetUserTableNames(connection);

                // ===== KROK 1: Domeny – UPEWNIJ SIĘ, że wszystkie ze skryptu istnieją =====
                // (żeby CREATE TABLE miał do czego się odwołać)
                foreach (var kvp in scriptDomains)
                {
                    var domName = kvp.Key;
                    if (!dbDomainNames.Contains(domName))
                    {
                        try
                        {
                            using var cmd = new FbCommand(kvp.Value, connection);
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"CREATE DOMAIN {domName}: {ex.Message}");
                        }
                    }
                }

                // odśwież listę domen po ewentualnym dodaniu
                dbDomainNames = GetUserDomainNames(connection);

                // ===== KROK 2: Procedury – najpierw stuby, potem DROP, potem FK =====
                // 2a) Zneutralizuj wszystkie procedury (ALTER ... AS BEGIN END),
                //     żeby straciły zależności od tabel / innych procedur.
                NeutralizeAllUserProcedures(connection, dbDomainNames);

                // 2b) Teraz można bezpiecznie je dropnąć (nie ma już zależności cyklicznych)
                DropAllUserProcedures(connection);

                // 2c) Dla porządku – jeśli są jakieś FK, też je zdejmujemy
                DropAllForeignKeys(connection);


                // ===== KROK 3: Tabele – diff po nazwach + porównanie kolumn =====

                // 3.1 Nadmiarowe tabele – kasujemy (są w bazie, nie ma ich w skryptach)
                dbTableNames = GetUserTableNames(connection); // świeża lista po ewentualnym DROP PROCEDURE
                foreach (var dbTable in dbTableNames)
                {
                    if (!scriptTables.ContainsKey(dbTable))
                    {
                        try
                        {
                            using var drop = new FbCommand($"DROP TABLE {dbTable}", connection);
                            drop.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"DROP TABLE {dbTable}: {ex.Message}");
                        }
                    }
                }

                // 3.2 Po dropie nadmiarowych – odśwież listę tabel
                dbTableNames = GetUserTableNames(connection);

                // 3.3 Dla każdej tabeli ze skryptu:
                //  - jeśli brak w bazie -> CREATE TABLE
                //  - jeśli istnieje -> porównaj definicje kolumn; jeśli różne -> DROP + CREATE
                foreach (var kvp in scriptTables)
                {
                    var tableName = kvp.Key;
                    var tableInfo = kvp.Value;

                    if (!dbTableNames.Contains(tableName))
                    {
                        // brakująca tabela – tworzymy
                        try
                        {
                            using var cmd = new FbCommand(tableInfo.CreateStatement, connection);
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            errors.Add($"CREATE TABLE {tableName}: {ex.Message}");
                        }
                    }
                    else
                    {
                        // istniejąca – porównaj definicje kolumn
                        bool differs = TableDefinitionDiffers(connection, tableName, dbDomainNames, tableInfo);
                        if (differs)
                        {
                            try
                            {
                                using (var drop = new FbCommand($"DROP TABLE {tableName}", connection))
                                {
                                    drop.ExecuteNonQuery();
                                }

                                using (var create = new FbCommand(tableInfo.CreateStatement, connection))
                                {
                                    create.ExecuteNonQuery();
                                }
                            }
                            catch (Exception ex)
                            {
                                errors.Add($"Recreate TABLE {tableName}: {ex.Message}");
                            }
                        }
                    }
                }

                // ===== KROK 4: Domeny – teraz możemy usunąć NADMIAROWE =====
                // (po wyrównaniu tabel finalny schemat nie powinien już ich używać)
                dbDomainNames = GetUserDomainNames(connection); // odśwież
                foreach (var dbDom in dbDomainNames)
                {
                    if (!scriptDomains.ContainsKey(dbDom))
                    {
                        try
                        {
                            using var drop = new FbCommand($"DROP DOMAIN {dbDom}", connection);
                            drop.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            // jeśli nadal coś ją trzyma, to jest realny problem – raportujemy
                            errors.Add($"DROP DOMAIN {dbDom}: {ex.Message}");
                        }
                    }
                }

                // ===== KROK 5: Procedury – wgrywamy pełny skrypt 03_procedures.sql =====
                if (!string.IsNullOrWhiteSpace(proceduresScriptText))
                {
                    try
                    {
                        var script = new FbScript(proceduresScriptText);
                        script.UnknownStatement += (sender, e) =>
                        {
                            e.NewStatementType = SqlStatementType.Update;
                            e.Handled = true;
                        };
                        script.Parse();

                        foreach (FbStatement stmt in script.Results)
                        {
                            var sql = stmt.Text;
                            if (string.IsNullOrWhiteSpace(sql))
                                continue;

                            sql = sql.Trim();
                            if (string.IsNullOrWhiteSpace(sql))
                                continue;

                            try
                            {
                                using var cmd = new FbCommand(sql, connection);
                                cmd.ExecuteNonQuery();
                            }
                            catch (FbException ex)
                            {
                                errors.Add($"Plik '03_procedures.sql': {ex.Message}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        errors.Add($"Plik '03_procedures.sql': {ex.Message}");
                    }
                }
            }

            if (errors.Count > 0)
            {
                Console.WriteLine("Błędy podczas aktualizacji bazy danych:");
                foreach (var err in errors)
                {
                    Console.WriteLine("  - " + err);
                }

                throw new InvalidOperationException("Aktualizacja bazy zakończona z błędami. Szczegóły powyżej.");
            }
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

                    sb.Append("CREATE DOMAIN ")
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

        private static string GenerateProceduresSql(FbConnection connection, HashSet<string> domainNames)
        {
            var procedures = new List<ProcedureMetadata>();

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
                    LoadProcedureParameters(connection, procName, domainNames, inputs, outputs);

                    var trimmedBody = body.Trim();
                    string bodyToEmit;

                    if (string.IsNullOrWhiteSpace(trimmedBody))
                    {
                        bodyToEmit = "AS\nBEGIN\nEND";
                    }
                    else
                    {
                        var noLeading = trimmedBody.TrimStart();
                        if (!noLeading.StartsWith("AS", StringComparison.OrdinalIgnoreCase))
                        {
                            bodyToEmit = "AS\n" + trimmedBody;
                        }
                        else
                        {
                            bodyToEmit = trimmedBody;
                        }
                    }

                    var meta = new ProcedureMetadata
                    {
                        Name = procName,
                        Body = bodyToEmit
                    };
                    meta.Inputs.AddRange(inputs);
                    meta.Outputs.AddRange(outputs);

                    procedures.Add(meta);
                }
            }

            if (procedures.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();

            sb.AppendLine("SET TERM ^ ;");
            sb.AppendLine();

            sb.AppendLine("-- === Stub definitions ===");
            foreach (var proc in procedures)
            {
                AppendProcedureDefinition(sb, "CREATE OR ALTER PROCEDURE", proc, isStub: true);
            }

            sb.AppendLine("-- === Full bodies ===");
            foreach (var proc in procedures)
            {
                AppendProcedureDefinition(sb, "ALTER PROCEDURE", proc, isStub: false);
            }

            sb.AppendLine("SET TERM ; ^");

            return sb.ToString();
        }

        private static void LoadProcedureParameters(
            FbConnection connection,
            string procedureName,
            HashSet<string> domainNames,
            List<string> inputs,
            List<string> outputs)
        {
            const string sql = @"
        SELECT
    TRIM(pp.RDB$PARAMETER_NAME) AS PARAM_NAME,
    pp.RDB$PARAMETER_TYPE,
    pp.RDB$PARAMETER_NUMBER,
    TRIM(f.RDB$FIELD_NAME)      AS FIELD_SOURCE,
    f.RDB$FIELD_TYPE,
    f.RDB$FIELD_SUB_TYPE,
    f.RDB$FIELD_LENGTH,
    f.RDB$FIELD_PRECISION,
    f.RDB$FIELD_SCALE,
    f.RDB$CHARACTER_LENGTH,
    cs.RDB$CHARACTER_SET_NAME,
    pp.RDB$DEFAULT_SOURCE       AS DEFAULT_SOURCE
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
                            typeDef = fieldSrc;
                        }
                        else
                        {
                            typeDef = GetSqlType(fieldType, fieldSubType, fieldScale, fieldPrec, charLen, charSet);
                        }

                        string defaultSource = GetNullableString(reader, "DEFAULT_SOURCE");

                        string def = $"{name} {typeDef}";

                        if (!string.IsNullOrWhiteSpace(defaultSource))
                        {
                            var defExpr = defaultSource.Trim();
                            def += " " + defExpr;
                        }

                        if (paramType == 0)
                            inputs.Add(def);
                        else
                            outputs.Add(def);
                    }
                }
            }
        }

        private static void AppendProcedureDefinition(
            StringBuilder sb,
            string keyword,              // "CREATE OR ALTER PROCEDURE" albo "ALTER PROCEDURE"
            ProcedureMetadata proc,
            bool isStub)
        {
            sb.Append(keyword).Append(' ').Append(proc.Name);

            // parametry wejściowe
            if (proc.Inputs.Count > 0)
            {
                sb.AppendLine()
                  .AppendLine("(");
                for (int i = 0; i < proc.Inputs.Count; i++)
                {
                    sb.Append("    ").Append(proc.Inputs[i]);
                    if (i < proc.Inputs.Count - 1)
                        sb.Append(',');
                    sb.AppendLine();
                }
                sb.AppendLine(")");
            }
            else
            {
                sb.AppendLine();
            }

            // parametry wyjściowe
            if (proc.Outputs.Count > 0)
            {
                sb.AppendLine("RETURNS (");
                for (int i = 0; i < proc.Outputs.Count; i++)
                {
                    sb.Append("    ").Append(proc.Outputs[i]);
                    if (i < proc.Outputs.Count - 1)
                        sb.Append(',');
                    sb.AppendLine();
                }
                sb.AppendLine(")");
            }

            if (isStub)
            {
                sb.AppendLine("AS");
                sb.AppendLine("BEGIN");
                sb.AppendLine("END");
                sb.AppendLine("^");
                sb.AppendLine();
            }
            else
            {
                sb.AppendLine(proc.Body.TrimEnd());
                sb.AppendLine("^");
                sb.AppendLine();
            }
        }
        #endregion

        private sealed class ProcedureMetadata
        {
            public string Name { get; set; } = string.Empty;
            public string Body { get; set; } = string.Empty;
            public List<string> Inputs { get; } = new List<string>();
            public List<string> Outputs { get; } = new List<string>();
        }

        private sealed class TableScriptInfo
        {
            public string Name { get; set; } = string.Empty;
            public string CreateStatement { get; set; } = string.Empty;
            public List<string> Columns { get; } = new List<string>();
        }

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

            return "BLOB";
        }

        // ======== NOWE / ZMIENIONE HELPERY DLA DIFF-A ========

        private static Dictionary<string, string> ParseDomainCreateStatements(string scriptText)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(scriptText))
                return result;

            var parts = scriptText.Split(';');
            foreach (var raw in parts)
            {
                var part = raw.Trim();
                if (part.Length == 0)
                    continue;

                if (!part.StartsWith("CREATE DOMAIN", StringComparison.OrdinalIgnoreCase))
                    continue;

                var tokens = part
                    .Split(new[] { ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

                if (tokens.Length < 3)
                    continue;

                var name = tokens[2].Trim('"');
                var stmt = part.TrimEnd() + ";";
                result[name] = stmt;
            }

            return result;
        }

        private static Dictionary<string, TableScriptInfo> ParseTableScripts(string scriptText)
        {
            var result = new Dictionary<string, TableScriptInfo>(StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(scriptText))
                return result;

            using var reader = new StringReader(scriptText);
            string? line;
            StringBuilder? currentStmt = null;
            TableScriptInfo? currentTable = null;

            while ((line = reader.ReadLine()) != null)
            {
                var trimmed = line.TrimStart();

                if (trimmed.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
                {
                    currentStmt = new StringBuilder();
                    currentStmt.AppendLine(line);

                    var tokens = trimmed
                        .Split(new[] { ' ', '\t', '(' }, StringSplitOptions.RemoveEmptyEntries);

                    if (tokens.Length >= 3)
                    {
                        var tableName = tokens[2].Trim('"');
                        currentTable = new TableScriptInfo { Name = tableName };
                    }
                    else
                    {
                        currentTable = null;
                    }

                    continue;
                }

                if (currentStmt != null)
                {
                    currentStmt.AppendLine(line);

                    if (trimmed.StartsWith(");", StringComparison.Ordinal))
                    {
                        if (currentTable != null)
                        {
                            currentTable.CreateStatement = currentStmt.ToString();
                            ExtractColumnsFromCreateTable(currentTable);
                            result[currentTable.Name] = currentTable;
                        }

                        currentStmt = null;
                        currentTable = null;
                    }
                }
            }

            return result;
        }

        private static void ExtractColumnsFromCreateTable(TableScriptInfo table)
        {
            var text = table.CreateStatement;
            int start = text.IndexOf('(');
            int end = text.LastIndexOf(");", StringComparison.Ordinal);

            if (start < 0 || end <= start)
                return;

            var inner = text.Substring(start + 1, end - start - 1);

            using var reader = new StringReader(inner);
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                var trimmed = line.Trim();
                if (string.IsNullOrEmpty(trimmed))
                    continue;

                if (trimmed.EndsWith(","))
                    trimmed = trimmed.Substring(0, trimmed.Length - 1).TrimEnd();

                if (trimmed.Length == 0)
                    continue;

                // zakładamy, że tu są tylko definicje kolumn – tak generuje je GenerateTablesSql
                table.Columns.Add(trimmed);
            }
        }

        private static HashSet<string> ParseProcedureNames(string scriptText)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(scriptText))
                return result;

            using var reader = new StringReader(scriptText);
            string? line;

            while ((line = reader.ReadLine()) != null)
            {
                var trimmed = line.TrimStart();
                if (trimmed.Length == 0)
                    continue;

                var tokens = trimmed
                    .Split(new[] { ' ', '\t', '(', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

                if (tokens.Length == 0)
                    continue;

                if (tokens[0].Equals("CREATE", StringComparison.OrdinalIgnoreCase))
                {
                    if (tokens.Length >= 3 &&
                        tokens[1].Equals("PROCEDURE", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Add(tokens[2].Trim('"'));
                    }
                    else if (tokens.Length >= 5 &&
                             tokens[1].Equals("OR", StringComparison.OrdinalIgnoreCase) &&
                             tokens[2].Equals("ALTER", StringComparison.OrdinalIgnoreCase) &&
                             tokens[3].Equals("PROCEDURE", StringComparison.OrdinalIgnoreCase))
                    {
                        result.Add(tokens[4].Trim('"'));
                    }
                }
                else if (tokens[0].Equals("ALTER", StringComparison.OrdinalIgnoreCase) &&
                         tokens.Length >= 3 &&
                         tokens[1].Equals("PROCEDURE", StringComparison.OrdinalIgnoreCase))
                {
                    result.Add(tokens[2].Trim('"'));
                }
            }

            return result;
        }


        private static bool TableDefinitionDiffers(
            FbConnection connection,
            string tableName,
            HashSet<string> dbDomainNames,
            TableScriptInfo scriptInfo)
        {
            var dbColumns = GetColumnsForTable(connection, tableName, dbDomainNames);
            var scriptColumns = scriptInfo.Columns;

            if (dbColumns.Count != scriptColumns.Count)
                return true;

            for (int i = 0; i < dbColumns.Count; i++)
            {
                var dbCol = dbColumns[i].Trim();
                var scriptCol = scriptColumns[i].Trim();

                if (!string.Equals(dbCol, scriptCol, StringComparison.OrdinalIgnoreCase))
                    return true;
            }

            return false;
        }

        private static HashSet<string> GetUserDomainNames(FbConnection connection)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            const string sql = @"
        SELECT TRIM(f.RDB$FIELD_NAME) AS DOMAIN_NAME
        FROM RDB$FIELDS f
        WHERE (f.RDB$SYSTEM_FLAG IS NULL OR f.RDB$SYSTEM_FLAG = 0)
          AND f.RDB$FIELD_NAME NOT LIKE 'RDB$%'";

            using var cmd = new FbCommand(sql, connection);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(reader.GetOrdinal("DOMAIN_NAME")).Trim();
                if (!string.IsNullOrEmpty(name))
                    result.Add(name);
            }

            return result;
        }

        private static HashSet<string> GetUserTableNames(FbConnection connection)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            const string sql = @"
        SELECT TRIM(r.RDB$RELATION_NAME) AS TABLE_NAME
        FROM RDB$RELATIONS r
        WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
          AND r.RDB$VIEW_BLR IS NULL";

            using var cmd = new FbCommand(sql, connection);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(reader.GetOrdinal("TABLE_NAME")).Trim();
                if (!string.IsNullOrEmpty(name))
                    result.Add(name);
            }

            return result;
        }

        private static HashSet<string> GetUserProcedureNames(FbConnection connection)
        {
            var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            const string sql = @"
        SELECT TRIM(p.RDB$PROCEDURE_NAME) AS PROC_NAME
        FROM RDB$PROCEDURES p
        WHERE COALESCE(p.RDB$SYSTEM_FLAG, 0) = 0";

            using var cmd = new FbCommand(sql, connection);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var name = reader.GetString(reader.GetOrdinal("PROC_NAME")).Trim();
                if (!string.IsNullOrEmpty(name))
                    result.Add(name);
            }

            return result;
        }

        private static void DropAllUserProcedures(FbConnection connection)
        {
            const string sql = @"
        SELECT TRIM(p.RDB$PROCEDURE_NAME) AS PROC_NAME
        FROM RDB$PROCEDURES p
        WHERE COALESCE(p.RDB$SYSTEM_FLAG, 0) = 0";

            var names = new List<string>();

            using (var cmd = new FbCommand(sql, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var name = reader.GetString(reader.GetOrdinal("PROC_NAME")).Trim();
                    if (!string.IsNullOrEmpty(name))
                        names.Add(name);
                }
            }

            foreach (var name in names)
            {
                using var drop = new FbCommand($"DROP PROCEDURE {name}", connection);
                try
                {
                    drop.ExecuteNonQuery();
                }
                catch
                {
                    // Ignorujemy ewentualne błędy pojedynczych procedur.
                    // I tak później odtworzymy komplet procedur ze skryptu 03_procedures.sql.
                }
            }
        }

        private static void DropAllForeignKeys(FbConnection connection)
        {
            const string sql = @"
        SELECT
            TRIM(rc.RDB$CONSTRAINT_NAME) AS CNAME,
            TRIM(rc.RDB$RELATION_NAME)   AS TNAME
        FROM RDB$RELATION_CONSTRAINTS rc
        WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'";

            var items = new List<(string Table, string Constraint)>();

            using (var cmd = new FbCommand(sql, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var cname = reader.GetString(reader.GetOrdinal("CNAME")).Trim();
                    var tname = reader.GetString(reader.GetOrdinal("TNAME")).Trim();

                    if (!string.IsNullOrEmpty(cname) && !string.IsNullOrEmpty(tname))
                    {
                        items.Add((tname, cname));
                    }
                }
            }

            foreach (var (table, constraint) in items)
            {
                var dropSql = $"ALTER TABLE {table} DROP CONSTRAINT {constraint}";
                using var dropCmd = new FbCommand(dropSql, connection);
                try
                {
                    dropCmd.ExecuteNonQuery();
                }
                catch
                {
                    // Ignorujemy pojedyncze błędy, bo i tak robimy pełny diff
                    // i finalnie struktura ma być zgodna ze skryptami.
                }
            }
        }

        private static void NeutralizeAllUserProcedures(FbConnection connection, HashSet<string> domainNames)
        {
            // 1) Pobierz listę wszystkich procedur użytkownika
            var procedures = new List<ProcedureMetadata>();

            const string sql = @"
        SELECT TRIM(p.RDB$PROCEDURE_NAME) AS PROCEDURE_NAME
        FROM RDB$PROCEDURES p
        WHERE COALESCE(p.RDB$SYSTEM_FLAG, 0) = 0";

            using (var cmd = new FbCommand(sql, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var name = reader.GetString(reader.GetOrdinal("PROCEDURE_NAME")).Trim();
                    if (string.IsNullOrEmpty(name))
                        continue;

                    var meta = new ProcedureMetadata { Name = name };

                    // używamy istniejącego helpera do wyciągnięcia parametrów
                    var inputs = new List<string>();
                    var outputs = new List<string>();
                    LoadProcedureParameters(connection, name, domainNames, inputs, outputs);

                    meta.Inputs.AddRange(inputs);
                    meta.Outputs.AddRange(outputs);

                    procedures.Add(meta);
                }
            }

            // 2) Dla każdej procedury generujemy ALTER ... z pustym ciałem
            foreach (var proc in procedures)
            {
                var sb = new StringBuilder();
                sb.Append("ALTER PROCEDURE ").Append(proc.Name);

                // parametry wejściowe
                if (proc.Inputs.Count > 0)
                {
                    sb.AppendLine()
                      .AppendLine("(");
                    for (int i = 0; i < proc.Inputs.Count; i++)
                    {
                        sb.Append("    ").Append(proc.Inputs[i]);
                        if (i < proc.Inputs.Count - 1)
                            sb.Append(',');
                        sb.AppendLine();
                    }
                    sb.AppendLine(")");
                }
                else
                {
                    sb.AppendLine();
                }

                // parametry wyjściowe
                if (proc.Outputs.Count > 0)
                {
                    sb.AppendLine("RETURNS (");
                    for (int i = 0; i < proc.Outputs.Count; i++)
                    {
                        sb.Append("    ").Append(proc.Outputs[i]);
                        if (i < proc.Outputs.Count - 1)
                            sb.Append(',');
                        sb.AppendLine();
                    }
                    sb.AppendLine(")");
                }

                sb.AppendLine("AS");
                sb.AppendLine("BEGIN");
                sb.AppendLine("END");

                var sqlStub = sb.ToString();

                using var cmdStub = new FbCommand(sqlStub, connection);
                try
                {
                    cmdStub.ExecuteNonQuery();
                }
                catch
                {
                    // jeśli jakaś pojedyncza procedura się nie „zneutralizuje”,
                    // trudno – większość zniknie z zależności i i tak odblokuje DROP TABLE.
                }
            }
        }





        #endregion
    }
}
