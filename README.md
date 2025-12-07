# Duch

Z powodu używania na codzień bazy Firebird 2.5 (x32) miałem problem z konfiguracją wersji Firebird 5.0.
Aplikacja jest budowana do wersji x32 bit (ograniczenie narzucone przez IBExpert)

Udało się wykonać zadanie na wersji Firebird 5.0 x32, ale do bazy łączę się lokalnie (bez uzycia serwera "remote" i localhosta)

komendy których używałem do wywołania programu (nie zmieniam ścieżek):

1. eksport skryptów z bazy:
   dotnet run -- export-scripts --connection-string "Database=C:\_BAZY\DUCH.FDB;User=SYSDBA;Password=masterkey;Dialect=3;Charset=UTF8;ServerType=1;ClientLibrary=C:\Program Files (x86)\Firebird\Firebird_5_0_32\fbclient.dll;" --output-dir "C:\_meta\scripts"

2. założenie bazy ze skryptów:
   dotnet run -- build-db --db-dir "C:\_BAZY\DUCH_kopia.fdb" --scripts-dir "C:\_meta\scripts"

3. aktualizacja bazy ze skryptów:
   dotnet run -- update-db --connection-string "Database=C:\_BAZY\DUCH.FDB;User=SYSDBA;Password=masterkey;Dialect=3;Charset=UTF8;ServerType=1;ClientLibrary=C:\Program Files (x86)\Firebird\Firebird_5_0_32\fbclient.dll;" --scripts-dir "C:\_meta\scripts"

W folderze \_BAZY jest baza oryginalna, skopiowana oraz zaktualizowana (kopia5).
W folderze scripts są skrypty wygenerowane z mojej bazy.
W folderze diffscripts są skrypty, które używałem do testów funkcji UpdateDatabase
