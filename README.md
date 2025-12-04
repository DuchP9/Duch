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

Punkt 3. (update) może być realizowany z użyciem skryptów wygenerowanych w punkcie 1. tylko w przypadku procedur ze względu na składnię (CREATE OR ALTER PROCEDURE.......), natomiast skrypty do aktualizacji tabel i domen trzeba dostosować (składnia skryptów CREATE TABLE....... i CREATE DOMAIN...........).
Nie do końca zrozumiałem czy aktualizacja musi odbywać się również z użyciem tych skryptów które wygenerowałem, więc zostawiam to w tej formie. Jeśli zamienimy ręcznie skrypt na "ALTER TABLE" lub "ALTER DOMAIN" to powinno zadziałać.

W folderze \_BAZY jest baza oryginalna oraz skopiowana.
W folderze scripts są skrypty wygenerowane z mojej bazy.
